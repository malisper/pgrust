//! Port of `src/backend/libpq/hba.c` (host-based authentication: tokenizing and
//! parsing `pg_hba.conf` / `pg_ident.conf`, and matching a connection against
//! the parsed rules) plus the SQL-view fill from
//! `src/backend/utils/adt/hbafuncs.c` (`fill_hba_view` / `fill_ident_view`).
//!
//! ## Modelling (the fabled model — peer of `backend-libpq-auth`)
//!
//!   * Data-derived buffers and parsed fields are plain owned `String` / `Vec`,
//!     exactly as `auth.c` does in this tree.
//!   * The shared connection/auth types (`Port`, `HbaLine`, `AuthToken`,
//!     `UserAuth`, `ConnType`, ...) come from `net` (defined by the merged
//!     `auth.c` port). `UserAuth`/`ConnType`/`IPCompareMethod`/`ClientCertMode`/
//!     `ClientCertName` are `u32` aliases compared with `==`.
//!   * `SockAddr` is the real `sockaddr_storage` byte buffer; family tests read
//!     `ss_family` from the bytes.
//!   * `ereport(elevel, ...)` is built with the real `backend-utils-error`
//!     builder and driven through `.finish(...)`, which raises at `ERROR`+ and
//!     logs-and-continues below — exactly the C `ereport` level semantics.
//!
//! ## Build-flag arms
//!
//! SSL/GSS/SSPI/PAM/BSD/LDAP/OpenLDAP are compile-time-off in this build, so
//! their `#ifdef` predicates are `false`-returning `const fn`s; the dead arms
//! (the OpenLDAP `ldapurl` parse) collapse to the not-supported branch, faithful
//! to a no-optional-features build.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

mod loaders;
mod matchers;
mod parse_hba;
mod parse_ident;
mod token;
mod tokenize;
mod views;

use std::cell::RefCell;

pub(crate) use ::utils_error::ereport;
use ::types_error::{
    ErrorLocation, PgResult, SqlState, DEBUG2, ERRCODE_CONFIG_FILE_ERROR,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_REGULAR_EXPRESSION, LOG,
};
use ::net::HbaLine;

pub(crate) use auth_seams as auth_seams;
pub(crate) use hba_seams as hba_seams;

pub use loaders::{
    check_hba, check_usermap, hba_authname, hba_getauthmethod, load_hba, load_ident,
};
pub use parse_hba::parse_hba_line;
pub use parse_ident::parse_ident_line;
pub use token::{free_auth_file, open_auth_file, pg_isblank};
pub use tokenize::tokenize_auth_file;

// ===========================================================================
// Constants (mirrored from the C headers).
// ===========================================================================

/// `C_COLLATION_OID` (`catalog/pg_collation_d.h`) — collation used to compile
/// the auth-file regexes. Value `950`.
pub const C_COLLATION_OID: types_core::Oid = 950;

/// `REG_ADVANCED` (`regex/regex.h`) — `000003` octal.
pub const REG_ADVANCED: i32 = 0o3;
/// `REG_OKAY` (`regex/regex.h`).
pub const REG_OKAY: i32 = 0;
/// `REG_NOMATCH` (`regex/regex.h`).
pub const REG_NOMATCH: i32 = 1;

/// `CONF_FILE_START_DEPTH` (`utils/conffiles.h`).
pub const CONF_FILE_START_DEPTH: i32 = 0;
/// `CONF_FILE_MAX_DEPTH` (`utils/conffiles.h`).
pub const CONF_FILE_MAX_DEPTH: i32 = 10;

/// `STATUS_OK` (`c.h`).
pub const STATUS_OK: i32 = 0;
/// `STATUS_ERROR` (`c.h`).
pub const STATUS_ERROR: i32 = -1;

/// `ENOENT` (errno.h) — "No such file or directory".
pub const ENOENT: i32 = 2;
/// `EAI_NONAME` (netdb.h) — `getaddrinfo` "name does not resolve".
pub const EAI_NONAME: i32 = libc::EAI_NONAME;

/// `src/backend/libpq/hba.c`, for `ErrorLocation`.
const SRCFILE_HBA: &str = "hba.c";

/// `ErrorLocation` for an `ereport(...)` raised from hba.c.
pub(crate) fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRCFILE_HBA, 0, funcname)
}

// ===========================================================================
// UserAuthName[] table (hba.c:102) + StaticAssertDecl (hba.c:125).
// ===========================================================================

/// `static const char *const UserAuthName[]` (hba.c:102). Indexed by the
/// [`UserAuth`] discriminant; keep in sync with the enum.
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

// ===========================================================================
// IdentLine / TokenizedAuthLine / CheckNetworkData (hba-private).
// ===========================================================================

/// `struct IdentLine` (`libpq/hba.h`) — one parsed `pg_ident.conf` mapping.
#[derive(Clone, Debug)]
pub struct IdentLine {
    /// `int linenumber`.
    pub linenumber: i32,
    /// `char *usermap`.
    pub usermap: String,
    /// `AuthToken *system_user`.
    pub system_user: ::net::AuthToken,
    /// `AuthToken *pg_user`.
    pub pg_user: ::net::AuthToken,
}

/// `struct TokenizedAuthLine` (`libpq/hba.h`) — one line lexed from an auth
/// config file. Each item of `fields` is a sub-list of [`AuthToken`].
#[derive(Clone, Debug, Default)]
pub struct TokenizedAuthLine {
    /// `List *fields` — list of lists of `AuthToken`.
    pub fields: Vec<Vec<::net::AuthToken>>,
    /// `char *file_name`.
    pub file_name: String,
    /// `int line_num`.
    pub line_num: i32,
    /// `char *raw_line`.
    pub raw_line: String,
    /// `char *err_msg` — error message if any (`NULL` => `None`).
    pub err_msg: Option<String>,
}

/// `struct check_network_data` (hba.c:56) — callback data for
/// `check_network_callback`.
pub(crate) struct CheckNetworkData {
    /// `IPCompareMethod method` — test method.
    pub method: ::net::IPCompareMethod,
    /// `bool result` — set to true if match.
    pub result: bool,
}

// ===========================================================================
// File-static parsed-line state (hba.c:80-94).
//
// A PostgreSQL backend is a single OS process with no internal threading; the
// `thread_local!` RefCell model (the repo convention for per-backend globals)
// is the faithful representation of these file-static lists. `load_hba` /
// `load_ident` replace them; `check_hba` / `check_usermap` read them.
// ===========================================================================

thread_local! {
    /// `static List *parsed_hba_lines = NIL;` (hba.c:86).
    static PARSED_HBA_LINES: RefCell<Vec<HbaLine>> = const { RefCell::new(Vec::new()) };
    /// `static List *parsed_ident_lines = NIL;` (hba.c:93).
    static PARSED_IDENT_LINES: RefCell<Vec<IdentLine>> = const { RefCell::new(Vec::new()) };
}

/// Replace `parsed_hba_lines`.
pub(crate) fn set_parsed_hba_lines(new: Vec<HbaLine>) {
    PARSED_HBA_LINES.with(|slot| *slot.borrow_mut() = new);
}

/// Replace `parsed_ident_lines`.
pub(crate) fn set_parsed_ident_lines(new: Vec<IdentLine>) {
    PARSED_IDENT_LINES.with(|slot| *slot.borrow_mut() = new);
}

// ===========================================================================
// AuthToken token-predicate helpers (hba.c:69-73).
//
// `AuthToken` is a foreign (`net`) type, so these are free functions
// taking `&AuthToken` rather than inherent methods.
//   token_has_regexp(t)         == (t->regex != NULL)
//   token_is_member_check(t)    == (!t->quoted && t->string[0] == '+')
//   token_is_keyword(t, k)      == (!t->quoted && strcmp(t->string, k) == 0)
//   token_matches(t, k)         == (strcmp(t->string, k) == 0)
//   token_matches_insensitive   == (pg_strcasecmp(t->string, k) == 0)
// ===========================================================================

/// `t->string` as bytes (C `string` is non-null after `make_auth_token`; the
/// `palloc0` zero state is the empty string).
pub(crate) fn tok_str(t: &::net::AuthToken) -> &[u8] {
    match &t.string {
        Some(s) => s.as_bytes(),
        None => b"",
    }
}

/// `token_has_regexp(t)` (hba.c:69).
pub(crate) fn token_has_regexp(t: &::net::AuthToken) -> bool {
    t.regex.is_some()
}

/// `token_is_member_check(t)` (hba.c:70).
pub(crate) fn token_is_member_check(t: &::net::AuthToken) -> bool {
    !t.quoted && tok_str(t).first() == Some(&b'+')
}

/// `token_is_keyword(t, k)` (hba.c:71).
pub(crate) fn token_is_keyword(t: &::net::AuthToken, k: &[u8]) -> bool {
    !t.quoted && tok_str(t) == k
}

/// `token_matches(t, k)` (hba.c:72).
pub(crate) fn token_matches(t: &::net::AuthToken, k: &[u8]) -> bool {
    tok_str(t) == k
}

/// `token_matches_insensitive(t, k)` (hba.c:73).
pub(crate) fn token_matches_insensitive(t: &::net::AuthToken, k: &[u8]) -> bool {
    pg_strcasecmp(tok_str(t), k) == 0
}

// ===========================================================================
// pg_strcasecmp (src/port/pgstrcasecmp.c).
// ===========================================================================

/// `int pg_strcasecmp(const char *s1, const char *s2)` (port/pgstrcasecmp.c).
/// ASCII-case-insensitive compare with C semantics (high-bit bytes compared as
/// `unsigned char`). The slices are token bodies (no embedded NUL); a length
/// difference at the common prefix mirrors the C `'\0'`-vs-non-`'\0'` compare.
pub fn pg_strcasecmp(s1: &[u8], s2: &[u8]) -> i32 {
    let n = s1.len().min(s2.len());
    for i in 0..n {
        let mut ch1 = s1[i];
        let mut ch2 = s2[i];
        if ch1 != ch2 {
            if ch1.is_ascii_uppercase() {
                ch1 += b'a' - b'A';
            }
            if ch2.is_ascii_uppercase() {
                ch2 += b'a' - b'A';
            }
            if ch1 != ch2 {
                return ch1 as i32 - ch2 as i32;
            }
        }
    }
    (s1.len() as i32) - (s2.len() as i32)
}

// ===========================================================================
// Build-flag predicates (#ifdef arms) — all off in this no-optional-features
// build, faithful to the compiled configuration.
// ===========================================================================

/// `USE_SSL` (build flag).
pub(crate) const fn use_ssl() -> bool {
    false
}
/// `EnableSSL` (GUC; only consulted under USE_SSL).
pub(crate) const fn enable_ssl() -> bool {
    false
}
/// `ENABLE_GSS` (build flag).
pub(crate) const fn enable_gss() -> bool {
    false
}
/// `ENABLE_SSPI` (build flag).
pub(crate) const fn enable_sspi() -> bool {
    false
}
/// `USE_PAM` (build flag).
pub(crate) const fn use_pam() -> bool {
    false
}
/// `USE_BSD_AUTH` (build flag).
pub(crate) const fn use_bsd_auth() -> bool {
    false
}
/// `USE_LDAP` (build flag).
pub(crate) const fn use_ldap() -> bool {
    false
}
/// `HAVE_LDAP_INITIALIZE` (configure probe).
pub(crate) const fn have_ldap_initialize() -> bool {
    false
}
/// `LDAP_API_FEATURE_X_OPENLDAP` (OpenLDAP build).
pub(crate) const fn ldap_api_feature_x_openldap() -> bool {
    false
}

// ===========================================================================
// Config-error reporting helpers.
//
// Each `hba.c` `ereport` has the shape
//   ereport(elevel, (errcode(code), errmsg(msg), [errhint(hint),]
//     errcontext("line %d of configuration file \"%s\"", line_num, file_name)))
// `.finish(here(...))?` raises at ERROR+ (propagated as Err) and
// logs-and-continues below ERROR, exactly the C `ereport` level semantics.
// ===========================================================================

/// `errcontext("line %d of configuration file \"%s\"", line_num, file_name)`.
pub(crate) fn line_context(line_num: i32, file_name: &str) -> String {
    format!("line {line_num} of configuration file \"{file_name}\"")
}

/// Emit a config-file `ereport(elevel, (errcode(F0000), errmsg(msg),
/// [errhint], errcontext(...)))`.
pub(crate) fn report_config(
    elevel: ::types_error::ErrorLevel,
    funcname: &'static str,
    msg: String,
    hint: Option<&str>,
    line_num: i32,
    file_name: &str,
) -> PgResult<()> {
    let mut b = ereport(elevel)
        .errcode(ERRCODE_CONFIG_FILE_ERROR)
        .errmsg(msg);
    if let Some(h) = hint {
        b = b.errhint(h.to_string());
    }
    b.errcontext_msg(line_context(line_num, file_name))
        .finish(here(funcname))
}

/// Emit `ereport(elevel, (errcode(code), errmsg(msg)))` (no errcontext, no
/// hint) — the plain log/error reports.
pub(crate) fn report_plain(
    elevel: ::types_error::ErrorLevel,
    funcname: &'static str,
    sqlstate: SqlState,
    msg: String,
) -> PgResult<()> {
    ereport(elevel).errcode(sqlstate).errmsg(msg).finish(here(funcname))
}

/// Emit `ereport(elevel, (errcode_for_file_access(), errmsg(msg)))` with the
/// errno-derived SQLSTATE and `%m` expansion (the file-open/read reports).
pub(crate) fn report_file_access(
    elevel: ::types_error::ErrorLevel,
    funcname: &'static str,
    save_errno: i32,
    msg: String,
    context: Option<(i32, &str)>,
) -> PgResult<()> {
    let mut b = ereport(elevel)
        .with_saved_errno(save_errno)
        .errcode_for_file_access()
        .errmsg(msg);
    if let Some((line_num, file_name)) = context {
        b = b.errcontext_msg(line_context(line_num, file_name));
    }
    b.finish(here(funcname))
}

// ===========================================================================
// init_seams — install the six seams consumed by auth.c and misc2.
// ===========================================================================

/// Install every seam this crate owns:
///   * `auth_seams`: `hba_getauthmethod`, `check_usermap`,
///     `hba_authname_of` (consumed by `auth.c`);
///   * `hba_seams`: `fill_hba_view`, `fill_ident_view`,
///     `hba_authname` (consumed by `misc2`).
pub fn init_seams() {
    auth_seams::hba_getauthmethod::set(loaders::hba_getauthmethod_entry);
    auth_seams::check_usermap::set(loaders::check_usermap_entry);
    auth_seams::hba_authname_of::set(loaders::hba_authname_of_entry);

    hba_seams::fill_hba_view::set(views::fill_hba_view_entry);
    hba_seams::fill_ident_view::set(views::fill_ident_view_entry);
    hba_seams::hba_authname::set(views::hba_authname_entry);

    // The postmaster (PostmasterMain) loads pg_hba.conf / pg_ident.conf at
    // startup. The C `load_hba()` / `load_ident()` return `bool` and log (do not
    // throw) parse failures; map an internal `Err` to the C `false` ("could not
    // load"). `HbaFileName` / `IdentFileName` are the configured paths used in
    // the postmaster's FATAL message.
    use postmaster_seams as psm;
    psm::load_hba::set(|| loaders::load_hba().unwrap_or(false));
    psm::load_ident::set(|| loaders::load_ident().unwrap_or(false));
    psm::hba_file_name::set(loaders::hba_file_name);
    psm::ident_file_name::set(loaders::ident_file_name);

    // Parallel-worker bring-up: InitializeSystemUser(authn_id, hba_authname(...))
    // once MyClientConnectionInfo is restored (parallel.c:1550). hba owns
    // hba_authname and already deps miscinit (InitializeSystemUser /
    // MyClientConnectionInfo); the parallel-rt seam crate is a leaf (no cycle).
    parallel_rt_seams::maybe_initialize_system_user::set(
        loaders::maybe_initialize_system_user,
    );
}

// A short-lived `MemoryContext`, the idiomatic stand-in for the implicit
// `CurrentMemoryContext` C uses (matching auth.c's `MemCtx`).
pub(crate) struct MemCtx(mcx::MemoryContext);

impl MemCtx {
    pub(crate) fn new(name: &'static str) -> Self {
        MemCtx(mcx::MemoryContext::new(name))
    }
    pub(crate) fn mcx(&self) -> mcx::Mcx<'_> {
        self.0.mcx()
    }
}

// Keep the otherwise-unused error-level / errcode imports referenced so the
// module-wide `use` list documents the report vocabulary in one place.
const _: (SqlState, SqlState, SqlState, ::types_error::ErrorLevel, ::types_error::ErrorLevel) = (
    ERRCODE_INTERNAL_ERROR,
    ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_REGULAR_EXPRESSION,
    DEBUG2,
    LOG,
);
