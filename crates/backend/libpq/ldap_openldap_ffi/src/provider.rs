//! OpenLDAP `libldap` FFI bindings + the `CheckLDAPAuth` flow (auth.c, USE_LDAP).

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

// ---------------------------------------------------------------------------
// OpenLDAP constants (ldap.h).
// ---------------------------------------------------------------------------

const LDAP_SUCCESS: c_int = 0;
const LDAP_VERSION3: c_int = 3;
const LDAP_OPT_PROTOCOL_VERSION: c_int = 0x0011;
const LDAP_OPT_DIAGNOSTIC_MESSAGE: c_int = 0x0032;
const LDAP_OPT_ERROR_NUMBER: c_int = 0x0031; // == LDAP_OPT_RESULT_CODE
/// `LDAP_NO_ATTRS` ("1.1"): request no attributes from the search.
const LDAP_NO_ATTRS: &[u8] = b"1.1\0";

#[allow(non_camel_case_types)]
type LDAP = c_void;
#[allow(non_camel_case_types)]
type LDAPMessage = c_void;

/// `LDAPURLDesc` (ldap.h): the parsed components of an LDAP URL. Field order
/// matches OpenLDAP exactly (used only as `*mut`, never constructed here).
#[repr(C)]
struct LDAPURLDesc {
    lud_next: *mut LDAPURLDesc,
    lud_scheme: *mut c_char,
    lud_host: *mut c_char,
    lud_port: c_int,
    lud_dn: *mut c_char,
    lud_attrs: *mut *mut c_char,
    lud_scope: c_int,
    lud_filter: *mut c_char,
    lud_exts: *mut *mut c_char,
    lud_crit_exts: c_int,
}

extern "C" {
    fn ldap_url_parse(url: *const c_char, ludpp: *mut *mut LDAPURLDesc) -> c_int;
    fn ldap_free_urldesc(ludp: *mut LDAPURLDesc);
    fn ldap_initialize(ldp: *mut *mut LDAP, url: *const c_char) -> c_int;
    fn ldap_set_option(ld: *mut LDAP, option: c_int, invalue: *const c_void) -> c_int;
    fn ldap_get_option(ld: *mut LDAP, option: c_int, outvalue: *mut c_void) -> c_int;
    fn ldap_start_tls_s(
        ld: *mut LDAP,
        serverctrls: *mut c_void,
        clientctrls: *mut c_void,
    ) -> c_int;
    fn ldap_simple_bind_s(ld: *mut LDAP, who: *const c_char, passwd: *const c_char) -> c_int;
    fn ldap_search_s(
        ld: *mut LDAP,
        base: *const c_char,
        scope: c_int,
        filter: *const c_char,
        attrs: *mut *mut c_char,
        attrsonly: c_int,
        res: *mut *mut LDAPMessage,
    ) -> c_int;
    fn ldap_count_entries(ld: *mut LDAP, chain: *mut LDAPMessage) -> c_int;
    fn ldap_first_entry(ld: *mut LDAP, chain: *mut LDAPMessage) -> *mut LDAPMessage;
    fn ldap_get_dn(ld: *mut LDAP, entry: *mut LDAPMessage) -> *mut c_char;
    fn ldap_msgfree(lm: *mut LDAPMessage) -> c_int;
    fn ldap_unbind(ld: *mut LDAP) -> c_int;
    fn ldap_memfree(p: *mut c_void);
    fn ldap_err2string(err: c_int) -> *const c_char;
}

// ---------------------------------------------------------------------------
// Public config + outcome.
// ---------------------------------------------------------------------------

/// Resolved hba LDAP options (a faithful subset of `HbaLine`'s ldap* fields).
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

pub enum LdapOutcome {
    /// Authenticated; carries the bind DN to record via `set_authn_id`.
    Ok(String),
    Error,
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

fn err2string(r: c_int) -> String {
    // SAFETY: ldap_err2string returns a static C string.
    unsafe {
        let p = ldap_err2string(r);
        if p.is_null() {
            String::new()
        } else {
            CStr::from_ptr(p).to_string_lossy().into_owned()
        }
    }
}

/// `errdetail_for_ldap(ldap)` (auth.c:2663): the LDAP diagnostic message, if any,
/// as an errdetail string ("LDAP diagnostics: ...").
fn errdetail_for_ldap(ldap: *mut LDAP) -> Option<String> {
    let mut message: *mut c_char = ptr::null_mut();
    // SAFETY: ldap is a live handle; we read the diagnostic-message option.
    let rc = unsafe {
        ldap_get_option(ldap, LDAP_OPT_DIAGNOSTIC_MESSAGE, &mut message as *mut _ as *mut c_void)
    };
    if rc == LDAP_SUCCESS && !message.is_null() {
        let s = unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned();
        unsafe { ldap_memfree(message as *mut c_void) };
        if s.is_empty() {
            None
        } else {
            Some(format!("LDAP diagnostics: {s}"))
        }
    } else {
        None
    }
}

/// `FormatSearchFilter` (auth.c:2413): replace every `$username` with `user_name`.
pub fn format_search_filter(pattern: &str, user_name: &str) -> String {
    pattern.replace("$username", user_name)
}

/// `InitializeLDAPConnection` (auth.c:2217) — the `HAVE_LDAP_INITIALIZE` path:
/// build a space-separated `scheme://host:port` URI list from `ldapserver`,
/// `ldap_initialize`, set protocol v3, and optionally start TLS.
///
/// The DNS-SRV branch (empty ldapserver -> ldap_dn2domain/ldap_domain2hostlist)
/// is not implemented; this build always has a server name when reaching here.
fn initialize_ldap_connection(cfg: &LdapConfig, logs: &mut Vec<String>) -> Option<*mut LDAP> {
    let scheme = cfg.ldapscheme.as_deref().unwrap_or("ldap");

    // Build "scheme://host:port" for each space-separated host in ldapserver.
    let server = cfg.ldapserver.as_deref().unwrap_or("");
    let mut uris = String::new();
    for host in server.split(' ').filter(|h| !h.is_empty()) {
        if !uris.is_empty() {
            uris.push(' ');
        }
        uris.push_str(scheme);
        uris.push_str("://");
        uris.push_str(host);
        uris.push_str(&format!(":{}", cfg.ldapport));
    }

    let c_uris = match CString::new(uris) {
        Ok(c) => c,
        Err(_) => {
            logs.push("could not initialize LDAP: invalid server URI".to_string());
            return None;
        }
    };

    let mut ldap: *mut LDAP = ptr::null_mut();
    let r = unsafe { ldap_initialize(&mut ldap, c_uris.as_ptr()) };
    if r != LDAP_SUCCESS {
        logs.push(format!("could not initialize LDAP: {}", err2string(r)));
        return None;
    }

    let ldapversion = LDAP_VERSION3;
    let r = unsafe {
        ldap_set_option(
            ldap,
            LDAP_OPT_PROTOCOL_VERSION,
            &ldapversion as *const c_int as *const c_void,
        )
    };
    if r != LDAP_SUCCESS {
        push_with_detail(
            logs,
            format!("could not set LDAP protocol version: {}", err2string(r)),
            ldap,
        );
        unsafe { ldap_unbind(ldap) };
        return None;
    }

    if cfg.ldaptls {
        let r = unsafe { ldap_start_tls_s(ldap, ptr::null_mut(), ptr::null_mut()) };
        if r != LDAP_SUCCESS {
            push_with_detail(
                logs,
                format!("could not start LDAP TLS session: {}", err2string(r)),
                ldap,
            );
            unsafe { ldap_unbind(ldap) };
            return None;
        }
    }

    Some(ldap)
}

/// Push a primary log line, appending the LDAP diagnostic detail on a second
/// line when available (C composes errmsg + errdetail_for_ldap into one
/// ereport; we keep them as one log entry).
fn push_with_detail(logs: &mut Vec<String>, msg: String, ldap: *mut LDAP) {
    match errdetail_for_ldap(ldap) {
        Some(d) => logs.push(format!("{msg}\nDETAIL:  {d}")),
        None => logs.push(msg),
    }
}

fn simple_bind(ldap: *mut LDAP, who: &str, passwd: &str) -> c_int {
    let c_who = CString::new(who).unwrap_or_default();
    let c_pw = CString::new(passwd).unwrap_or_default();
    unsafe { ldap_simple_bind_s(ldap, c_who.as_ptr(), c_pw.as_ptr()) }
}

// ---------------------------------------------------------------------------
// CheckLDAPAuth (auth.c:2436).
// ---------------------------------------------------------------------------

/// `CheckLDAPAuth(port)` (auth.c:2436), minus the password-recv (the caller
/// supplies `password`, already obtained via sendAuthRequest +
/// recv_password_packet) and minus `set_authn_id` (the caller owns it,
/// recording the returned `LdapOutcome::Ok(dn)`).
///
/// Returns the outcome and the LOG lines the caller should `ereport(LOG)`.
pub fn check_ldap_auth(
    cfg: &LdapConfig,
    user_name: &str,
    password: &str,
) -> Result<(LdapOutcome, Vec<String>), String> {
    let mut logs: Vec<String> = Vec::new();

    // server_name: empty string when using SRV records (no server name).
    let server_name = cfg.ldapserver.as_deref().unwrap_or("");

    let ldap = match initialize_ldap_connection(cfg, &mut logs) {
        Some(l) => l,
        None => return Ok((LdapOutcome::Error, logs)),
    };

    let fulluser: String;

    if let Some(basedn) = cfg.ldapbasedn.as_deref() {
        // --- Search+bind mode ---

        // Disallow LDAP-filter metacharacters in the username.
        if user_name.chars().any(|c| matches!(c, '*' | '(' | ')' | '\\' | '/')) {
            logs.push("invalid character in user name for LDAP authentication".to_string());
            unsafe { ldap_unbind(ldap) };
            return Ok((LdapOutcome::Error, logs));
        }

        // Bind with ldapbinddn/ldapbindpasswd (anonymous if absent).
        let binddn = cfg.ldapbinddn.as_deref().unwrap_or("");
        let bindpw = cfg.ldapbindpasswd.as_deref().unwrap_or("");
        let r = simple_bind(ldap, binddn, bindpw);
        if r != LDAP_SUCCESS {
            push_with_detail(
                &mut logs,
                format!(
                    "could not perform initial LDAP bind for ldapbinddn \"{binddn}\" on server \"{server_name}\": {}",
                    err2string(r)
                ),
                ldap,
            );
            unsafe { ldap_unbind(ldap) };
            return Ok((LdapOutcome::Error, logs));
        }

        // Build the search filter.
        let filter = if let Some(f) = cfg.ldapsearchfilter.as_deref() {
            format_search_filter(f, user_name)
        } else if let Some(attr) = cfg.ldapsearchattribute.as_deref() {
            format!("({attr}={user_name})")
        } else {
            format!("(uid={user_name})")
        };

        let c_base = CString::new(basedn).unwrap_or_default();
        let c_filter = match CString::new(filter.clone()) {
            Ok(c) => c,
            Err(_) => {
                unsafe { ldap_unbind(ldap) };
                return Ok((LdapOutcome::Error, logs));
            }
        };
        // attributes[] = { LDAP_NO_ATTRS, NULL }
        let mut attrs: [*mut c_char; 2] =
            [LDAP_NO_ATTRS.as_ptr() as *mut c_char, ptr::null_mut()];

        let mut search_message: *mut LDAPMessage = ptr::null_mut();
        let r = unsafe {
            ldap_search_s(
                ldap,
                c_base.as_ptr(),
                cfg.ldapscope,
                c_filter.as_ptr(),
                attrs.as_mut_ptr(),
                0,
                &mut search_message,
            )
        };
        if r != LDAP_SUCCESS {
            push_with_detail(
                &mut logs,
                format!(
                    "could not search LDAP for filter \"{filter}\" on server \"{server_name}\": {}",
                    err2string(r)
                ),
                ldap,
            );
            if !search_message.is_null() {
                unsafe { ldap_msgfree(search_message) };
            }
            unsafe { ldap_unbind(ldap) };
            return Ok((LdapOutcome::Error, logs));
        }

        let count = unsafe { ldap_count_entries(ldap, search_message) };
        if count != 1 {
            if count == 0 {
                logs.push(format!(
                    "LDAP user \"{user_name}\" does not exist\nDETAIL:  LDAP search for filter \"{filter}\" on server \"{server_name}\" returned no entries."
                ));
            } else {
                let plural = if count == 1 { "entry" } else { "entries" };
                logs.push(format!(
                    "LDAP user \"{user_name}\" is not unique\nDETAIL:  LDAP search for filter \"{filter}\" on server \"{server_name}\" returned {count} {plural}."
                ));
            }
            unsafe { ldap_unbind(ldap) };
            unsafe { ldap_msgfree(search_message) };
            return Ok((LdapOutcome::Error, logs));
        }

        let entry = unsafe { ldap_first_entry(ldap, search_message) };
        let dn = unsafe { ldap_get_dn(ldap, entry) };
        if dn.is_null() {
            let mut error: c_int = 0;
            unsafe {
                ldap_get_option(
                    ldap,
                    LDAP_OPT_ERROR_NUMBER,
                    &mut error as *mut c_int as *mut c_void,
                )
            };
            push_with_detail(
                &mut logs,
                format!(
                    "could not get dn for the first entry matching \"{filter}\" on server \"{server_name}\": {}",
                    err2string(error)
                ),
                ldap,
            );
            unsafe { ldap_unbind(ldap) };
            unsafe { ldap_msgfree(search_message) };
            return Ok((LdapOutcome::Error, logs));
        }

        fulluser = unsafe { CStr::from_ptr(dn) }.to_string_lossy().into_owned();
        unsafe { ldap_memfree(dn as *mut c_void) };
        unsafe { ldap_msgfree(search_message) };
    } else {
        // --- Simple-bind mode ---
        fulluser = format!(
            "{}{}{}",
            cfg.ldapprefix.as_deref().unwrap_or(""),
            user_name,
            cfg.ldapsuffix.as_deref().unwrap_or(""),
        );
    }

    // Final bind as the resolved DN with the user's password.
    let r = simple_bind(ldap, &fulluser, password);
    if r != LDAP_SUCCESS {
        push_with_detail(
            &mut logs,
            format!(
                "LDAP login failed for user \"{fulluser}\" on server \"{server_name}\": {}",
                err2string(r)
            ),
            ldap,
        );
        unsafe { ldap_unbind(ldap) };
        return Ok((LdapOutcome::Error, logs));
    }

    unsafe { ldap_unbind(ldap) };
    Ok((LdapOutcome::Ok(fulluser), logs))
}

// ---------------------------------------------------------------------------
// ldapurl parsing (hba.c:2194, LDAP_API_FEATURE_X_OPENLDAP path).
// ---------------------------------------------------------------------------

/// The fields hba.c copies out of a parsed `ldapurl` into the HbaLine
/// (hba.c:2226-2238): scheme, host, port, dn (basedn), the first search
/// attribute, scope, and filter.
#[derive(Debug)]
pub struct ParsedLdapUrl {
    pub scheme: Option<String>,
    pub host: Option<String>,
    pub port: i32,
    pub basedn: Option<String>,
    pub searchattribute: Option<String>,
    pub scope: i32,
    pub filter: Option<String>,
}

/// `ldap_url_parse(val, &urldata)` + the scheme check (hba.c:2203-2222). On
/// success returns the parsed fields; on failure returns the error message
/// hba.c would emit (caller raises the config error).
pub fn parse_ldap_url(url: &str) -> Result<ParsedLdapUrl, String> {
    let c_url = CString::new(url).map_err(|_| format!("could not parse LDAP URL \"{url}\""))?;
    let mut urldata: *mut LDAPURLDesc = ptr::null_mut();
    let rc = unsafe { ldap_url_parse(c_url.as_ptr(), &mut urldata) };
    if rc != LDAP_SUCCESS {
        return Err(format!("could not parse LDAP URL \"{url}\": {}", err2string(rc)));
    }

    // SAFETY: urldata is a live, non-null LDAPURLDesc on LDAP_SUCCESS.
    let ud = unsafe { &*urldata };

    let scheme = cstr_opt(ud.lud_scheme);
    // unsupported scheme check.
    match scheme.as_deref() {
        Some("ldap") | Some("ldaps") => {}
        Some(other) => {
            let msg = format!("unsupported LDAP URL scheme: {other}");
            unsafe { ldap_free_urldesc(urldata) };
            return Err(msg);
        }
        None => {
            unsafe { ldap_free_urldesc(urldata) };
            return Err("unsupported LDAP URL scheme: (none)".to_string());
        }
    }

    // Only the first search attribute is used (hba.c:2235).
    let searchattribute = if ud.lud_attrs.is_null() {
        None
    } else {
        let first = unsafe { *ud.lud_attrs };
        cstr_opt(first)
    };

    let parsed = ParsedLdapUrl {
        scheme,
        host: cstr_opt(ud.lud_host),
        port: ud.lud_port,
        basedn: cstr_opt(ud.lud_dn),
        searchattribute,
        scope: ud.lud_scope,
        filter: cstr_opt(ud.lud_filter),
    };
    unsafe { ldap_free_urldesc(urldata) };
    Ok(parsed)
}

/// Copy a possibly-NULL C string into an owned `Option<String>`.
fn cstr_opt(p: *mut c_char) -> Option<String> {
    if p.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
    }
}
