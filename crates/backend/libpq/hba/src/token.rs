//! Token-level primitives of `hba.c`: `pg_isblank`, `next_token`, the
//! `AuthToken` constructors/copiers (`make_auth_token`, `free_auth_token`,
//! `copy_auth_token`), the regex helpers (`regcomp_auth_token`,
//! `regexec_auth_token`), and the file-open/close primitives (`open_auth_file`,
//! `free_auth_file`).
//!
//! Ported from `src/backend/libpq/hba.c` (lines 145-660).
//!
//! ## File model
//!
//! The C uses an `AllocateFile`/`pg_get_line_append`/`feof`/`ferror` sequential
//! `FILE *`. This tree's fd owner exposes `allocate_file_read(path)`, which
//! reads the whole text file into a byte buffer (`Ok(None)` on `ENOENT`). An
//! auth config file is small text, so [`FileHandle`] carries the whole content
//! and the tokenizer iterates its lines — behavior-equivalent to the C
//! line-at-a-time read, with no opaque stdio-stream handle.

use fd_seams as fd;
use ::types_error::{ErrorLevel, PgResult};
use ::net::AuthToken;
use ::regex::{RegMatch, RegcompResult, RegexCompiled, RegexecResult};

use crate::{
    here, report_file_access, tok_str, token_has_regexp, C_COLLATION_OID, CONF_FILE_MAX_DEPTH,
    ENOENT, MemCtx, REG_ADVANCED, REG_NOMATCH, REG_OKAY,
};

/// An opened auth file: its whole content plus the recursion depth it was
/// opened at (the C `FILE *` + the implicit `depth` cleanup level).
pub struct FileHandle {
    /// The file's bytes (`AllocateFile` + read-to-EOF).
    pub content: Vec<u8>,
    /// `depth` the file was opened at (passed to `free_auth_file`).
    pub depth: i32,
}

/// `bool pg_isblank(const char c)` (hba.c:145).
#[inline]
pub fn pg_isblank(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\r'
}

/// `static bool next_token(char **lineptr, StringInfo buf, bool *initial_quote,
/// bool *terminating_comma)` (hba.c:186).
///
/// Grab one token out of `line` starting at `*pos`, dequoting and skipping
/// comments. Stores the dequoted token in `buf`, advances `*pos`, and reports
/// `initial_quote` / `terminating_comma`. Returns `true` if a token (or a
/// quote) was seen.
pub(crate) fn next_token(
    line: &[u8],
    pos: &mut usize,
    buf: &mut Vec<u8>,
    initial_quote: &mut bool,
    terminating_comma: &mut bool,
) -> bool {
    let mut c: u8;
    let mut in_quote = false;
    let mut was_quote = false;
    let mut saw_quote = false;

    // Models the C `c = (*(*lineptr)++)`: read the byte at the cursor (the C
    // string is NUL-terminated, so reading past the content yields '\0'), then
    // advance the cursor.
    macro_rules! getc {
        () => {{
            let ch = line.get(*pos).copied().unwrap_or(0);
            *pos += 1;
            ch
        }};
    }

    // Initialize output parameters.
    buf.clear(); // resetStringInfo(buf)
    *initial_quote = false;
    *terminating_comma = false;

    // Move over any whitespace and commas preceding the next token.
    loop {
        c = getc!();
        if c == 0 || !(pg_isblank(c) || c == b',') {
            break;
        }
    }

    // Build a token in buf of next characters up to EOL, unquoted comma, or
    // unquoted whitespace.
    while c != 0 && (!pg_isblank(c) || in_quote) {
        // skip comments to EOL
        if c == b'#' && !in_quote {
            loop {
                c = getc!();
                if c == 0 {
                    break;
                }
            }
            break;
        }

        // we do not pass back a terminating comma in the token
        if c == b',' && !in_quote {
            *terminating_comma = true;
            break;
        }

        if c != b'"' || was_quote {
            buf.push(c); // appendStringInfoChar(buf, c)
        }

        // Literal double-quote is two double-quotes.
        if in_quote && c == b'"' {
            was_quote = !was_quote;
        } else {
            was_quote = false;
        }

        if c == b'"' {
            in_quote = !in_quote;
            saw_quote = true;
            if buf.is_empty() {
                *initial_quote = true;
            }
        }

        c = getc!();
    }

    // Un-eat the char right after the token (critical in case it is '\0', else
    // next call will read past end of string).
    *pos -= 1;

    saw_quote || !buf.is_empty()
}

/// `static AuthToken *make_auth_token(const char *token, bool quoted)`
/// (hba.c:258). Construct an [`AuthToken`] copying `token`.
pub(crate) fn make_auth_token(token: &[u8], quoted: bool) -> AuthToken {
    AuthToken {
        string: Some(String::from_utf8_lossy(token).into_owned()),
        quoted,
        regex: None,
    }
}

/// `static void free_auth_token(AuthToken *token)` (hba.c:279). Free the
/// token's compiled regex, if any (`pg_regfree`).
pub(crate) fn free_auth_token(token: &mut AuthToken) {
    if token_has_regexp(token) {
        // pg_regfree(token->regex). Mirroring C, the handle is not cleared.
        if let Some(re) = token.regex.take() {
            regex_core_seams::pg_regfree::call(re);
        }
    }
}

/// `static AuthToken *copy_auth_token(AuthToken *in)` (hba.c:289). Copy a token
/// into fresh memory (string + quoted; the regex is *not* copied, matching C).
pub(crate) fn copy_auth_token(input: &AuthToken) -> AuthToken {
    make_auth_token(tok_str(input), input.quoted)
}

/// `static int regcomp_auth_token(AuthToken *token, char *filename, int
/// line_num, char **err_msg, int elevel)` (hba.c:302).
///
/// Compile the token's regex (if its string starts with `/`) and store it in
/// `token.regex`. Returns the `pg_regcomp` result (`0` == ok / nothing to
/// compile, non-zero == compile error); on error, sets `*err_msg`.
pub(crate) fn regcomp_auth_token(
    token: &mut AuthToken,
    filename: &str,
    line_num: i32,
    err_msg: &mut Option<String>,
    elevel: ErrorLevel,
) -> PgResult<i32> {
    // Assert(token->regex == NULL);
    debug_assert!(token.regex.is_none());

    let s = tok_str(token);
    if s.first() != Some(&b'/') {
        return Ok(0); // nothing to compile
    }

    // C: token->regex = palloc0(...); wstr = pg_mb2wchar_with_len(string+1);
    //    rc = pg_regcomp(token->regex, wstr, wlen, REG_ADVANCED, C_COLLATION_OID).
    let pat = &s[1..];
    let pat_owned = pat.to_vec();
    let mcx = MemCtx::new("regcomp_auth_token");
    let wstr = mbutils_seams::pg_mb2wchar_with_len::call(mcx.mcx(), &pat_owned)?;

    match regex_core_seams::pg_regcomp::call(&wstr, REG_ADVANCED, C_COLLATION_OID)? {
        RegcompResult::Compiled(c) => {
            token.regex = Some(c);
            Ok(REG_OKAY)
        }
        RegcompResult::Failed(f) => {
            // The carrier already holds the `pg_regerror`-formatted message.
            let pat_str = String::from_utf8_lossy(pat);
            // ereport(elevel, (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
            //   errmsg("invalid regular expression \"%s\": %s", string+1, errstr),
            //   errcontext("line %d of configuration file \"%s\"", ...)))
            let msg = format!("invalid regular expression \"{pat_str}\": {}", f.message);
            crate::ereport(elevel)
                .errcode(::types_error::ERRCODE_INVALID_REGULAR_EXPRESSION)
                .errmsg(msg.clone())
                .errcontext_msg(crate::line_context(line_num, filename))
                .finish(here("regcomp_auth_token"))?;
            // *err_msg = psprintf(...)
            *err_msg = Some(msg);
            // C returns the nonzero pg_regcomp rc; any nonzero is a failure.
            Ok(1)
        }
    }
}

/// `static int regexec_auth_token(const char *match, AuthToken *token, size_t
/// nmatch, regmatch_t pmatch[])` (hba.c:347).
///
/// Execute the token's compiled regex against `m`. Returns `(rc, matches,
/// errstr)`: `rc` is `REG_OKAY` / `REG_NOMATCH` / `1` (any other failure, with
/// the formatted message in `errstr`); `matches` has `nmatch` entries.
pub(crate) fn regexec_auth_token(
    m: &[u8],
    token: &AuthToken,
    nmatch: usize,
) -> PgResult<(i32, Vec<RegMatch>, Option<String>)> {
    // Assert(token->string[0] == '/' && token->regex);
    debug_assert!(tok_str(token).first() == Some(&b'/') && token.regex.is_some());
    let re: &RegexCompiled = token.regex.as_ref().expect("regexec_auth_token: regex is NULL");

    let m_owned = m.to_vec();
    let mcx = MemCtx::new("regexec_auth_token");
    let wmatch = mbutils_seams::pg_mb2wchar_with_len::call(mcx.mcx(), &m_owned)?;

    let mut pmatch = vec![RegMatch::UNSET; nmatch];
    match regex_core_seams::pg_regexec::call(re, &wmatch, 0, &mut pmatch)? {
        RegexecResult::Matched => Ok((REG_OKAY, pmatch, None)),
        RegexecResult::NoMatch => Ok((REG_NOMATCH, pmatch, None)),
        RegexecResult::Failed(f) => Ok((1, pmatch, Some(f.message))),
    }
}

/// `void free_auth_file(FILE *file, int depth)` (hba.c:571). Free a file opened
/// by [`open_auth_file`].
///
/// In C this also drops the per-load `tokenize_context` at
/// `CONF_FILE_START_DEPTH`; here the per-tokenization allocations are owned
/// `Vec`s dropped on scope exit, so only the file release boundary remains
/// (the in-memory content is dropped with the handle).
pub fn free_auth_file(_file: FileHandle, _depth: i32) {
    // FreeFile(file): the whole-file buffer is owned by `file` and dropped here.
}

/// `FILE *open_auth_file(const char *filename, int elevel, int depth, char
/// **err_msg)` (hba.c:596).
///
/// Open the given file (reading its whole content), rejecting too-deep nesting.
/// Returns the handle, or `None` on failure (with `*err_msg` set).
pub fn open_auth_file(
    filename: &str,
    elevel: ErrorLevel,
    depth: i32,
    err_msg: &mut Option<String>,
) -> PgResult<Option<FileHandle>> {
    // Reject too-deep include nesting depth.
    if depth > CONF_FILE_MAX_DEPTH {
        // ereport(elevel, (errcode_for_file_access(),
        //   errmsg("could not open file \"%s\": maximum nesting depth exceeded", filename)))
        let msg = format!("could not open file \"{filename}\": maximum nesting depth exceeded");
        // errcode_for_file_access() with no errno yields the generic file-access
        // SQLSTATE; pass errno 0 (no `%m` in this message).
        report_file_access(elevel, "open_auth_file", 0, msg.clone(), None)?;
        *err_msg = Some(msg);
        return Ok(None);
    }

    // file = AllocateFile(filename, "r");  (read-to-EOF model)
    // allocate_file_read returns Ok(None) on ENOENT and raises ERROR on any
    // other open/read failure (fd-owned); the dominant paths (present file /
    // missing include) match the C exactly.
    let content = fd::allocate_file_read::call(filename)?;
    match content {
        Some(content) => Ok(Some(FileHandle { content, depth })),
        None => {
            // errno == ENOENT: the file does not exist.
            let save_errno = ENOENT;
            // ereport(elevel, (errcode_for_file_access(),
            //   errmsg("could not open file \"%s\": %m", filename)))
            report_file_access(
                elevel,
                "open_auth_file",
                save_errno,
                format!("could not open file \"{filename}\": %m"),
                None,
            )?;
            // if (err_msg) *err_msg = psprintf("could not open file \"%s\": %m", filename);
            *err_msg = Some(format!(
                "could not open file \"{filename}\": {}",
                strerror(save_errno)
            ));
            Ok(None)
        }
    }
}

/// `%m` expansion for the `err_msg` string (the emitted message uses the error
/// builder's `with_saved_errno`; this is only for the recorded `err_msg`).
fn strerror(errnum: i32) -> &'static str {
    match errnum {
        ENOENT => "No such file or directory",
        13 => "Permission denied",
        _ => "I/O error",
    }
}
