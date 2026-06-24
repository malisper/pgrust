//! FFI provider for `CheckLDAPAuth` (auth.c, `#ifdef USE_LDAP`): LDAP
//! authentication against the system OpenLDAP `libldap`.
//!
//! This binds the SAME OpenLDAP `libldap`/`liblber` PostgreSQL's `--with-ldap`
//! links — never a reimplementation of the LDAP protocol. It exposes
//! `check_ldap_auth`, which runs the full auth.c flow over real `libldap`:
//!
//!   * `InitializeLDAPConnection` — build the `scheme://host:port` URI list
//!     (the `HAVE_LDAP_INITIALIZE` / `ldap_initialize` path), set protocol v3,
//!     and optionally `ldap_start_tls_s` (`ldaptls=1`).
//!   * Simple bind (`ldapprefix`/`ldapsuffix`): bind DN = prefix+user+suffix,
//!     bound with the user's password.
//!   * Search+bind (`ldapbasedn`): bind as `ldapbinddn` (or anonymous), search
//!     with `ldapsearchfilter` / `ldapsearchattribute` / default `(uid=%s)`,
//!     require exactly one entry, then re-bind as the found DN with the user's
//!     password.
//!
//! The caller (auth.c port) supplies the resolved hba options, the client
//! password, and the LOG/ereport mapping; it owns `set_authn_id`.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

#[cfg(all(feature = "with-ldap", not(target_family = "wasm")))]
mod provider;

#[cfg(all(test, feature = "with-ldap", not(target_family = "wasm")))]
mod tests;

#[cfg(all(feature = "with-ldap", not(target_family = "wasm")))]
pub use provider::{
    check_ldap_auth, format_search_filter, parse_ldap_url, LdapConfig, LdapOutcome, ParsedLdapUrl,
};

/// `USE_LDAP` / `HAVE_LDAP_INITIALIZE` — whether this build links OpenLDAP. Read
/// by the hba parser to accept or reject the `ldap` method, and to allow the
/// SRV-record path (empty ldapserver with an ldapbasedn).
pub const fn ldap_available() -> bool {
    cfg!(all(feature = "with-ldap", not(target_family = "wasm")))
}

/// `LDAP_API_FEATURE_X_OPENLDAP` — whether this build links OpenLDAP, which
/// provides `ldap_url_parse` for the `ldapurl` hba option. Read by the hba
/// parser to choose the URL-parse arm over the "not supported" arm.
pub const fn ldap_api_feature_x_openldap() -> bool {
    cfg!(all(feature = "with-ldap", not(target_family = "wasm")))
}

/// `LDAP_PORT` (ldap.h): the default plaintext LDAP port.
pub const LDAP_PORT: i32 = 389;
/// `LDAPS_PORT` (ldap.h): the default LDAPS port.
pub const LDAPS_PORT: i32 = 636;
/// `LDAP_SCOPE_SUBTREE` (ldap.h): the default `ldapscope` for search+bind.
pub const LDAP_SCOPE_SUBTREE: i32 = 2;

// --- Stubs when OpenLDAP is not compiled in --------------------------------

/// Resolved hba LDAP options (a faithful subset of `HbaLine`'s ldap* fields).
#[cfg(not(all(feature = "with-ldap", not(target_family = "wasm"))))]
#[derive(Default)]
pub struct LdapConfig {
    pub ldapscheme: Option<String>,
    pub ldapserver: Option<String>,
    pub ldapport: i32,
    pub ldaptls: bool,
    pub ldapbasedn: Option<String>,
    pub ldapbinddn: Option<String>,
    pub ldapbindpasswd: Option<String>,
    pub ldapsearchattribute: Option<String>,
    pub ldapsearchfilter: Option<String>,
    pub ldapscope: i32,
    pub ldapprefix: Option<String>,
    pub ldapsuffix: Option<String>,
}

#[cfg(not(all(feature = "with-ldap", not(target_family = "wasm"))))]
pub enum LdapOutcome {
    /// Authenticated; carries the bind DN to record via `set_authn_id`.
    Ok(String),
    Error,
}

#[cfg(not(all(feature = "with-ldap", not(target_family = "wasm"))))]
pub fn check_ldap_auth(
    _cfg: &LdapConfig,
    _user_name: &str,
    _password: &str,
) -> Result<(LdapOutcome, Vec<String>), String> {
    unreachable!("CheckLDAPAuth reached without USE_LDAP (hba should have rejected `ldap`)")
}

/// `FormatSearchFilter` (auth.c:2413) — replace `$username` in `pattern` with
/// `user_name`. Exposed for unit testing even without OpenLDAP linked.
#[cfg(not(all(feature = "with-ldap", not(target_family = "wasm"))))]
pub fn format_search_filter(pattern: &str, user_name: &str) -> String {
    pattern.replace("$username", user_name)
}

/// Parsed `ldapurl` fields (stub when OpenLDAP is not linked).
#[cfg(not(all(feature = "with-ldap", not(target_family = "wasm"))))]
pub struct ParsedLdapUrl {
    pub scheme: Option<String>,
    pub host: Option<String>,
    pub port: i32,
    pub basedn: Option<String>,
    pub searchattribute: Option<String>,
    pub scope: i32,
    pub filter: Option<String>,
}

/// `ldap_url_parse` (stub): without OpenLDAP, the hba parser takes the "LDAP
/// URLs not supported on this platform" arm and never calls this.
#[cfg(not(all(feature = "with-ldap", not(target_family = "wasm"))))]
pub fn parse_ldap_url(_url: &str) -> Result<ParsedLdapUrl, String> {
    unreachable!("parse_ldap_url reached without OpenLDAP (hba should report not-supported)")
}
