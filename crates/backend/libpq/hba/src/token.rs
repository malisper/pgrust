use elog::ereport;
use types_error::{ErrorLevel, PgResult};
use types_startup::AuthToken;

use crate::{loc, CONF_FILE_MAX_DEPTH};

// C reads via AllocateFile + pg_get_line_append; auth config files are small
// text, so the handle carries the whole content (guc_file precedent) and the
// tokenizer walks its lines.
pub struct FileHandle {
    pub content: Vec<u8>,
    pub depth: i32,
}

#[inline]
pub fn pg_isblank(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\r'
}

pub(crate) fn next_token(
    line: &[u8],
    pos: &mut usize,
    buf: &mut Vec<u8>,
    initial_quote: &mut bool,
    terminating_comma: &mut bool,
) -> bool {
    let mut in_quote = false;
    let mut was_quote = false;
    let mut saw_quote = false;

    // c = *(*lineptr)++ — reads past content yield the NUL terminator.
    macro_rules! getc {
        () => {{
            let ch = line.get(*pos).copied().unwrap_or(0);
            *pos += 1;
            ch
        }};
    }

    buf.clear();
    *initial_quote = false;
    *terminating_comma = false;

    let mut c: u8;
    loop {
        c = getc!();
        if c == 0 || !(pg_isblank(c) || c == b',') {
            break;
        }
    }

    while c != 0 && (!pg_isblank(c) || in_quote) {
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
            buf.push(c);
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

    // Un-eat the char after the token so the next call starts on it.
    *pos -= 1;

    saw_quote || !buf.is_empty()
}

pub(crate) fn make_auth_token(token: &[u8], quoted: bool) -> AuthToken {
    AuthToken {
        string: String::from_utf8_lossy(token).into_owned(),
        quoted,
        regex: false,
    }
}

pub(crate) fn copy_auth_token(input: &AuthToken) -> AuthToken {
    // C's copy_auth_token drops the compiled regex (make_auth_token copy).
    AuthToken { regex: false, ..input.clone() }
}

// regcomp_auth_token (hba.c:302): '/'-prefixed tokens compile a regex
// (validation; the token keeps the compiled-OK marker); non-zero return =
// compile failure (already reported at elevel, err_msg set for the
// pg_hba_file_rules/pg_ident_file_mappings views).
pub(crate) fn regcomp_auth_token(
    token: &mut AuthToken,
    filename: &str,
    line_num: i32,
    err_msg: &mut Option<String>,
    elevel: ErrorLevel,
) -> PgResult<i32> {
    use ::regex::{RegcompResult, REG_ADVANCED};

    debug_assert!(!token.regex);
    if token.string.as_bytes().first() != Some(&b'/') {
        return Ok(0); // nothing to compile
    }

    let pattern = &token.string.as_bytes()[1..];
    let cx = mcx::MemoryContext::new("regcomp_auth_token");
    let wstr = mbutils_seams::pg_mb2wchar_with_len::call(cx.mcx(), pattern)?;
    match regex_core_seams::pg_regcomp::call(&wstr, REG_ADVANCED, types_core::catalog::C_COLLATION_OID)? {
        RegcompResult::Compiled(re) => {
            // Parsed auth lines are shared across threads; the Rc-based
            // carrier cannot live in them. Validation happened; the executing
            // thread recompiles in regexec_auth_token.
            regex_core_seams::pg_regfree::call(re);
            token.regex = true;
            Ok(0)
        }
        RegcompResult::Failed(f) => {
            let msg = format!(
                "invalid regular expression \"{}\": {}",
                &token.string[1..],
                f.message
            );
            ereport(elevel)
                .errcode(types_error::ERRCODE_INVALID_REGULAR_EXPRESSION)
                .errmsg(msg.clone())
                .errcontext_msg(crate::line_context(line_num, filename))
                .finish(loc(327, "regcomp_auth_token"))?;
            *err_msg = Some(msg);
            Ok(1)
        }
    }
}

// regexec_auth_token (hba.c:348): Ok(true) = match, Ok(false) = REG_NOMATCH,
// Err(msg) = hard regexec failure (the caller raises its own ereport).
pub(crate) fn regexec_auth_token(
    matchstr: &str,
    token: &AuthToken,
    pmatch: &mut [::regex::RegMatch],
) -> PgResult<Result<bool, String>> {
    use ::regex::{RegcompResult, RegexecResult, REG_ADVANCED};

    debug_assert!(token.regex && token.string.starts_with('/'));
    let cx = mcx::MemoryContext::new("regexec_auth_token");
    let wpat =
        mbutils_seams::pg_mb2wchar_with_len::call(cx.mcx(), &token.string.as_bytes()[1..])?;
    // Recompile of a parse-validated pattern (see regcomp_auth_token).
    let re = match regex_core_seams::pg_regcomp::call(
        &wpat,
        REG_ADVANCED,
        types_core::catalog::C_COLLATION_OID,
    )? {
        RegcompResult::Compiled(re) => re,
        RegcompResult::Failed(f) => {
            panic!("regexec_auth_token: parse-validated pattern failed to recompile: {}", f.message)
        }
    };
    let wstr = mbutils_seams::pg_mb2wchar_with_len::call(cx.mcx(), matchstr.as_bytes())?;
    let result = match regex_core_seams::pg_regexec::call(&re, &wstr, 0, pmatch)? {
        RegexecResult::Matched => Ok(true),
        RegexecResult::NoMatch => Ok(false),
        RegexecResult::Failed(f) => Err(f.message),
    };
    regex_core_seams::pg_regfree::call(re);
    Ok(result)
}

pub fn free_auth_file(_file: FileHandle, _depth: i32) {}

pub fn open_auth_file(
    filename: &str,
    elevel: ErrorLevel,
    depth: i32,
    err_msg: &mut Option<String>,
) -> PgResult<Option<FileHandle>> {
    if depth > CONF_FILE_MAX_DEPTH {
        let msg = format!("could not open file \"{filename}\": maximum nesting depth exceeded");
        ereport(elevel)
            .errcode_for_file_access()
            .errmsg(msg.clone())
            .finish(loc(608, "open_auth_file"))?;
        *err_msg = Some(msg);
        return Ok(None);
    }

    match std::fs::read(filename) {
        Ok(content) => Ok(Some(FileHandle { content, depth })),
        Err(e) => {
            let errno = e.raw_os_error().unwrap_or(0);
            ereport(elevel)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{filename}\": %m"))
                .finish(loc(620, "open_auth_file"))?;
            *err_msg = Some(format!(
                "could not open file \"{filename}\": {}",
                elog::errno::strerror(errno)
            ));
            crate::tokenize::set_last_open_errno(errno);
            Ok(None)
        }
    }
}
