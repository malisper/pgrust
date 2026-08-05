//! hba.c: pg_hba.conf / pg_ident.conf tokenizing, parsing, and connection
//! matching. Scope: the line-parse/match engine with the trust / reject /
//! password-family / peer / ident / cert keywords live (cert is hostssl-only
//! and forces clientcert=verify-full, as in C's USE_SSL build). Regex auth
//! tokens compile and match via the house regex engine. Deferred loud:
//! radius / oauth method parse. gss / sspi / pam / bsd / ldap are rejected
//! as "not supported by this build", faithful to a no-GSS/SSPI/PAM/BSD/LDAP
//! C build.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

mod check;
mod parse_hba;
mod parse_ident;
#[cfg(test)]
mod tests;
mod token;
mod tokenize;


use elog::ereport;
pub use pgstrcasecmp::pg_strcasecmp;
use types_core::init::UserAuth;
use types_error::{ErrorLevel, ErrorLocation, PgResult, SqlState, ERRCODE_CONFIG_FILE_ERROR, LOG};
use types_startup::{AuthToken, HbaLine, Port};

pub use check::check_hba;
pub use parse_hba::parse_hba_line;
pub use parse_ident::{check_ident_usermap, parse_ident_line, IdentLine};
pub use token::{free_auth_file, open_auth_file, pg_isblank, FileHandle};
pub use tokenize::tokenize_auth_file;

pub const CONF_FILE_START_DEPTH: i32 = 0;
pub const CONF_FILE_MAX_DEPTH: i32 = 10;

pub const STATUS_OK: i32 = 0;
pub const STATUS_ERROR: i32 = -1;

pub const MAX_TOKEN: usize = 256;

pub static USER_AUTH_NAME: [&str; 16] = [
    "reject",
    "implicit reject", // Not a user-visible option
    "trust",
    "ident",
    "password",
    "md5",
    "scram-sha-256",
    "gss",
    "sspi",
    "pam",
    "bsd",
    "ldap",
    "cert",
    "radius",
    "peer",
    "oauth",
];

const _: () = assert!(USER_AUTH_NAME.len() == types_core::init::uaOAuth as usize + 1);

pub(crate) fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/libpq/hba.c", line, func)
}

#[derive(Clone, Debug, Default)]
pub struct TokenizedAuthLine {
    pub fields: Vec<Vec<AuthToken>>,
    pub file_name: String,
    pub line_num: i32,
    pub raw_line: String,
    pub err_msg: Option<String>,
}

// C's parsed_hba_lines live in the postmaster and reach backends via fork;
// the thread rendering is a process global (loads at startup/SIGHUP on the
// postmaster thread, each backend reads once at auth).
static PARSED_HBA_LINES: std::sync::RwLock<Vec<HbaLine>> = std::sync::RwLock::new(Vec::new());
static PARSED_IDENT_LINES: std::sync::RwLock<Vec<IdentLine>> = std::sync::RwLock::new(Vec::new());

pub(crate) fn with_parsed_hba_lines<R>(f: impl FnOnce(&[HbaLine]) -> R) -> R {
    f(&PARSED_HBA_LINES.read().unwrap_or_else(|e| e.into_inner()))
}

pub(crate) fn with_parsed_ident_lines<R>(f: impl FnOnce(&[IdentLine]) -> R) -> R {
    f(&PARSED_IDENT_LINES.read().unwrap_or_else(|e| e.into_inner()))
}

pub(crate) fn token_is_member_check(t: &AuthToken) -> bool {
    !t.quoted && t.string.as_bytes().first() == Some(&b'+')
}

pub(crate) fn token_is_keyword(t: &AuthToken, k: &str) -> bool {
    !t.quoted && t.string == k
}

pub(crate) fn token_matches(t: &AuthToken, k: &[u8]) -> bool {
    t.string.as_bytes() == k
}

pub(crate) fn token_matches_insensitive(t: &AuthToken, k: &[u8]) -> bool {
    pg_strcasecmp(t.string.as_bytes(), k) == 0
}

pub(crate) fn token_has_regexp(t: &AuthToken) -> bool {
    t.regex
}

pub(crate) fn line_context(line_num: i32, file_name: &str) -> String {
    format!("line {line_num} of configuration file \"{file_name}\"")
}

pub(crate) fn report_config(
    elevel: ErrorLevel,
    cline: i32,
    func: &'static str,
    msg: String,
    hint: Option<&str>,
    line_num: i32,
    file_name: &str,
) -> PgResult<()> {
    let mut b = ereport(elevel).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(msg);
    if let Some(h) = hint {
        b = b.errhint(h);
    }
    b.errcontext_msg(line_context(line_num, file_name))
        .finish(loc(cline, func))
}

pub(crate) fn report_plain(
    elevel: ErrorLevel,
    cline: i32,
    func: &'static str,
    sqlstate: SqlState,
    msg: String,
) -> PgResult<()> {
    ereport(elevel).errcode(sqlstate).errmsg(msg).finish(loc(cline, func))
}

pub fn load_hba() -> PgResult<bool> {
    let hba_file_name = hba_file_name();

    let mut open_err = None;
    let Some(file) = token::open_auth_file(&hba_file_name, LOG, 0, &mut open_err)? else {
        return Ok(false); // error already logged
    };

    let mut hba_lines: Vec<TokenizedAuthLine> = Vec::new();
    let mut new_parsed_lines: Vec<HbaLine> = Vec::new();
    let mut ok = true;

    tokenize_auth_file(&hba_file_name, &file, &mut hba_lines, LOG, 0)?;

    for tok_line in hba_lines.iter_mut() {
        if tok_line.err_msg.is_some() {
            ok = false;
            continue;
        }
        match parse_hba_line(tok_line, LOG)? {
            None => {
                // Parse error; keep parsing the rest of the file.
                ok = false;
                continue;
            }
            Some(newline) => new_parsed_lines.push(newline),
        }
    }

    if ok && new_parsed_lines.is_empty() {
        report_plain(
            LOG,
            2698,
            "load_hba",
            ERRCODE_CONFIG_FILE_ERROR,
            format!("configuration file \"{hba_file_name}\" contains no entries"),
        )?;
        ok = false;
    }

    if !ok {
        return Ok(false);
    }

    *PARSED_HBA_LINES.write().unwrap_or_else(|e| e.into_inner()) = new_parsed_lines;
    Ok(true)
}

pub fn load_ident() -> PgResult<bool> {
    let ident_file_name = ident_file_name();

    // not FATAL ... we just won't do any special ident maps.
    let mut open_err = None;
    let Some(file) = token::open_auth_file(&ident_file_name, LOG, 0, &mut open_err)? else {
        return Ok(false);
    };

    let mut ident_lines: Vec<TokenizedAuthLine> = Vec::new();
    let mut new_parsed_lines: Vec<IdentLine> = Vec::new();
    let mut ok = true;

    tokenize_auth_file(&ident_file_name, &file, &mut ident_lines, LOG, 0)?;

    for tok_line in ident_lines.iter_mut() {
        if tok_line.err_msg.is_some() {
            ok = false;
            continue;
        }
        match parse_ident_line(tok_line, LOG)? {
            None => {
                ok = false;
                continue;
            }
            Some(newline) => new_parsed_lines.push(newline),
        }
    }

    if !ok {
        return Ok(false);
    }

    *PARSED_IDENT_LINES.write().unwrap_or_else(|e| e.into_inner()) = new_parsed_lines;
    Ok(true)
}

pub fn hba_getauthmethod(port: &mut Port) -> PgResult<()> {
    check_hba(port)
}

pub fn hba_authname(auth_method: UserAuth) -> &'static str {
    USER_AUTH_NAME[auth_method as usize]
}

pub fn check_usermap(
    usermap_name: Option<&str>,
    pg_user: &str,
    system_user: &str,
    case_insensitive: bool,
) -> PgResult<i32> {
    let mut found_entry = false;
    let mut error = false;

    if usermap_name.is_none_or(str::is_empty) {
        if case_insensitive {
            if pg_strcasecmp(pg_user.as_bytes(), system_user.as_bytes()) == 0 {
                return Ok(STATUS_OK);
            }
        } else if pg_user == system_user {
            return Ok(STATUS_OK);
        }
        report_plain(
            LOG,
            2984,
            "check_usermap",
            types_error::ERRCODE_INTERNAL_ERROR,
            format!(
                "provided user name ({pg_user}) and authenticated user name ({system_user}) do not match"
            ),
        )?;
        return Ok(STATUS_ERROR);
    }

    let usermap_name = usermap_name.expect("checked non-empty");
    with_parsed_ident_lines(|lines| {
        for ident_line in lines {
            let (found, err) = check_ident_usermap(
                ident_line,
                usermap_name,
                pg_user,
                system_user,
                case_insensitive,
            )?;
            found_entry = found;
            error = err;
            if found_entry || error {
                break;
            }
        }
        PgResult::Ok(())
    })?;

    if !found_entry && !error {
        report_plain(
            LOG,
            3009,
            "check_usermap",
            types_error::ERRCODE_INTERNAL_ERROR,
            format!(
                "no match in usermap \"{usermap_name}\" for user \"{pg_user}\" authenticated as \"{system_user}\""
            ),
        )?;
    }

    Ok(if found_entry { STATUS_OK } else { STATUS_ERROR })
}

pub fn hba_file_name() -> String {
    guc_tables::vars::HbaFileName.read().unwrap_or_default()
}

pub fn ident_file_name() -> String {
    guc_tables::vars::IdentFileName.read().unwrap_or_default()
}

pub fn init_seams() {
    hba_seams::load_hba::set(|| load_hba().unwrap_or(false));
    hba_seams::load_ident::set(|| load_ident().unwrap_or(false));
    auth_seams::load_hba::set(|| load_hba().unwrap_or(false));
    auth_seams::load_ident::set(|| load_ident().unwrap_or(false));
    hba_seams::hba_authname::set(hba_authname);
}
