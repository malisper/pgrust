//! The top-level loaders and entry points of `hba.c`: `check_hba`, `load_hba` /
//! `load_ident`, `hba_getauthmethod`, `hba_authname`, `check_usermap`, plus the
//! seam entry points consumed by `auth.c`.
//!
//! Ported from `src/backend/libpq/hba.c` (lines 2530-3126).

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use acl_seams as acl;
use ::mcx::{Mcx, PgString};
use ::types_error::{PgResult, DEBUG3, LOG};
use ::types_core::{uaImplicitReject, UserAuth};
use ::net::{
    ctHostGSS, ctHostNoGSS, ctHostNoSSL, ctHostSSL, ctLocal, ipCmpAll, ipCmpMask, ipCmpSameHost,
    ipCmpSameNet, HbaLine, Port,
};

use crate::matchers::{
    check_db, check_hostname, check_ip, check_role, check_same_host_or_net, ss_family,
};
use crate::parse_hba::parse_hba_line;
use crate::parse_ident::{check_ident_usermap, parse_ident_line};
use crate::token::{free_auth_file, open_auth_file};
use crate::tokenize::tokenize_auth_file;
use crate::{
    enable_gss, report_plain, set_parsed_hba_lines, set_parsed_ident_lines, IdentLine,
    TokenizedAuthLine, PARSED_HBA_LINES, PARSED_IDENT_LINES, STATUS_ERROR, STATUS_OK,
    USER_AUTH_NAME,
};

/// `addr`/`mask` views of an `HbaLine` for the IP matchers.
fn addr_view(h: &HbaLine) -> ::net::SockAddr {
    ::net::SockAddr { addr: h.addr, salen: h.addrlen as u32 }
}
fn mask_view(h: &HbaLine) -> ::net::SockAddr {
    ::net::SockAddr { addr: h.mask, salen: h.masklen as u32 }
}

/// `static void check_hba(hbaPort *port)` (hba.c:2530). Scan the pre-parsed hba
/// lines for a match to `port`, setting `port.hba` (the matched line, or an
/// implicit-reject line if none).
pub fn check_hba(port: &mut Port) -> PgResult<()> {
    // roleid = get_role_oid(port->user_name, true);
    let user_name = port.user_name.clone().unwrap_or_default();
    let roleid = acl::get_role_oid::call(&user_name, true)?;
    let dbname = port.database_name.clone().unwrap_or_default();

    // Snapshot the parsed lines (cloned out of the thread-local to avoid holding
    // the RefCell borrow across the matcher calls, which read other state).
    let lines: Vec<HbaLine> = PARSED_HBA_LINES.with(|s| s.borrow().clone());

    for hba in &lines {
        // Check connection type.
        if hba.conntype == ctLocal {
            if ss_family(&port.raddr) != libc::AF_UNIX {
                continue;
            }
        } else {
            if ss_family(&port.raddr) == libc::AF_UNIX {
                continue;
            }

            // Check SSL state.
            if port.ssl_in_use {
                // SSL: match both "host" and "hostssl".
                if hba.conntype == ctHostNoSSL {
                    continue;
                }
            } else {
                // not SSL: match both "host" and "hostnossl".
                if hba.conntype == ctHostSSL {
                    continue;
                }
            }

            // Check GSSAPI state.
            if enable_gss() {
                // #ifdef ENABLE_GSS — gss_enc is always false in this build.
                let gss_enc = false;
                if gss_enc && hba.conntype == ctHostNoGSS {
                    continue;
                } else if !gss_enc && hba.conntype == ctHostGSS {
                    continue;
                }
            } else {
                // #else
                if hba.conntype == ctHostGSS {
                    continue;
                }
            }

            // Check IP address.
            if hba.ip_cmp_method == ipCmpMask {
                if let Some(hostname) = hba.hostname.as_deref() {
                    if !check_hostname(port, hostname.as_bytes())? {
                        continue;
                    }
                } else if !check_ip(&port.raddr, &addr_view(hba), &mask_view(hba)) {
                    continue;
                }
            } else if hba.ip_cmp_method == ipCmpAll {
                // matches anything
            } else if hba.ip_cmp_method == ipCmpSameHost || hba.ip_cmp_method == ipCmpSameNet {
                if !check_same_host_or_net(&port.raddr, hba.ip_cmp_method)? {
                    continue;
                }
            } else {
                // shouldn't get here, but deem it no-match if so.
                continue;
            }
        } // != ctLocal

        // Check database and role.
        if !check_db(dbname.as_bytes(), user_name.as_bytes(), roleid, &hba.databases)? {
            continue;
        }
        if !check_role(user_name.as_bytes(), roleid, &hba.roles, false)? {
            continue;
        }

        // Found a record that matched!
        port.hba = Some(Box::new(hba.clone()));
        return Ok(());
    }

    // No matching entry: implicitly reject.
    let mut hba = HbaLine::new_zeroed();
    hba.auth_method = uaImplicitReject;
    port.hba = Some(Box::new(hba));
    Ok(())
}

/// `bool load_hba(void)` (hba.c:2644). Read `pg_hba.conf` and replace
/// `parsed_hba_lines` if it parses cleanly. Returns `false` on any parse error.
pub fn load_hba() -> PgResult<bool> {
    let hba_file_name = hba_file_name();

    let mut open_err = None;
    let file = match open_auth_file(&hba_file_name, LOG, 0, &mut open_err)? {
        Some(f) => f,
        None => return Ok(false), // error already logged
    };

    let mut hba_lines: Vec<TokenizedAuthLine> = Vec::new();
    let mut new_parsed_lines: Vec<HbaLine> = Vec::new();
    let mut ok = true;

    tokenize_auth_file(&hba_file_name, &file, &mut hba_lines, LOG, 0)?;

    // Now parse all the lines.
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

    // A valid HBA file must have at least one entry.
    if ok && new_parsed_lines.is_empty() {
        report_plain(
            LOG,
            "load_hba",
            ::types_error::ERRCODE_CONFIG_FILE_ERROR,
            format!("configuration file \"{hba_file_name}\" contains no entries"),
        )?;
        ok = false;
    }

    free_auth_file(file, 0);

    if !ok {
        return Ok(false);
    }

    // Loaded new file successfully, replace the one we use.
    set_parsed_hba_lines(new_parsed_lines);
    Ok(true)
}

/// `bool load_ident(void)` (hba.c:3020).
pub fn load_ident() -> PgResult<bool> {
    let ident_file_name = ident_file_name();

    // not FATAL ... we just won't do any special ident maps.
    let mut open_err = None;
    let file = match open_auth_file(&ident_file_name, LOG, 0, &mut open_err)? {
        Some(f) => f,
        None => return Ok(false), // error already logged
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

    free_auth_file(file, 0);

    if !ok {
        return Ok(false);
    }

    set_parsed_ident_lines(new_parsed_lines);
    Ok(true)
}

/// `void hba_getauthmethod(hbaPort *port)` (hba.c:3109).
pub fn hba_getauthmethod(port: &mut Port) -> PgResult<()> {
    check_hba(port)
}

/// `const char *hba_authname(UserAuth auth_method)` (hba.c:3122).
pub fn hba_authname(auth_method: UserAuth) -> &'static str {
    USER_AUTH_NAME[auth_method as usize]
}

/// `int check_usermap(const char *usermap_name, const char *pg_user, const char
/// *system_user, bool case_insensitive)` (hba.c:2965).
pub fn check_usermap(
    usermap_name: Option<&[u8]>,
    pg_user: &[u8],
    system_user: &[u8],
    case_insensitive: bool,
) -> PgResult<i32> {
    let mut found_entry = false;
    let mut error = false;

    // if (usermap_name == NULL || usermap_name[0] == '\0')
    if usermap_name.is_none() || usermap_name == Some(&b""[..]) {
        if case_insensitive {
            if crate::pg_strcasecmp(pg_user, system_user) == 0 {
                return Ok(STATUS_OK);
            }
        } else if pg_user == system_user {
            return Ok(STATUS_OK);
        }
        // ereport(LOG, (errmsg("provided user name (%s) and authenticated user
        //   name (%s) do not match", pg_user, system_user)))
        let pg = String::from_utf8_lossy(pg_user);
        let su = String::from_utf8_lossy(system_user);
        report_plain(
            LOG,
            "check_usermap",
            ::types_error::ERRCODE_INTERNAL_ERROR,
            format!("provided user name ({pg}) and authenticated user name ({su}) do not match"),
        )?;
        return Ok(STATUS_ERROR);
    }

    let usermap_name_b = usermap_name.expect("usermap_name checked non-empty above");

    // Snapshot the parsed ident lines (avoid holding the RefCell across calls).
    let lines: Vec<IdentLine> = PARSED_IDENT_LINES.with(|s| s.borrow().clone());
    for ident_line in &lines {
        let (found, err) = check_ident_usermap(
            ident_line,
            usermap_name_b,
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

    if !found_entry && !error {
        let um = String::from_utf8_lossy(usermap_name_b);
        let pg = String::from_utf8_lossy(pg_user);
        let su = String::from_utf8_lossy(system_user);
        report_plain(
            LOG,
            "check_usermap",
            ::types_error::ERRCODE_INTERNAL_ERROR,
            format!("no match in usermap \"{um}\" for user \"{pg}\" authenticated as \"{su}\""),
        )?;
    }

    Ok(if found_entry { STATUS_OK } else { STATUS_ERROR })
}

// ---------------------------------------------------------------------------
// GUC file names (HbaFileName / IdentFileName).
// ---------------------------------------------------------------------------

/// `HbaFileName` (guc) — the configured `hba_file` path.
pub(crate) fn hba_file_name() -> String {
    misc_guc::live::get_string("hba_file")
        .flatten()
        .unwrap_or_default()
}

/// `IdentFileName` (guc) — the configured `ident_file` path.
pub(crate) fn ident_file_name() -> String {
    misc_guc::live::get_string("ident_file")
        .flatten()
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// auth.c seam entry points.
// ---------------------------------------------------------------------------

/// `hba_getauthmethod(port)` seam (auth.c consumes it): run `check_hba` against
/// the caller-supplied `Port`. C threads the live `Port *` down from
/// `ClientAuthentication` (auth.c:390); the caller already holds the port (taken
/// out of the `MyProcPort` cell by `client_authentication`'s `with_my_proc_port`
/// frame), so a re-entrant ambient `MyProcPort` read here would observe it unset.
pub(crate) fn hba_getauthmethod_entry(port: &mut ::net::Port) -> PgResult<()> {
    hba_getauthmethod(port)
}

/// `check_usermap(usermap_name, pg_role, auth_user, case_insensitive)` seam.
pub(crate) fn check_usermap_entry(
    usermap_name: Option<String>,
    pg_role: String,
    auth_user: String,
    case_insensitive: bool,
) -> PgResult<i32> {
    check_usermap(
        usermap_name.as_deref().map(|s| s.as_bytes()),
        pg_role.as_bytes(),
        auth_user.as_bytes(),
        case_insensitive,
    )
}

/// `hba_authname_of(mcx, method)` seam — `USER_AUTH_NAME[method]` as `PgString`.
pub(crate) fn hba_authname_of_entry<'mcx>(
    mcx: Mcx<'mcx>,
    method: UserAuth,
) -> PgResult<PgString<'mcx>> {
    PgString::from_str_in(hba_authname(method), mcx)
}

/// Parallel-worker bring-up: initialize `SystemUser` once `MyClientConnectionInfo`
/// is restored (parallel.c:1550-1555):
/// ```c
/// if (MyClientConnectionInfo.authn_id)
///     InitializeSystemUser(MyClientConnectionInfo.authn_id,
///                          hba_authname(MyClientConnectionInfo.auth_method));
/// ```
/// Owned by hba (the home of `hba_authname`), which already deps miscinit (the
/// home of `MyClientConnectionInfo` + `InitializeSystemUser`); miscinit cannot
/// dep hba (hba→miscinit), so this conditional lives here.
pub(crate) fn maybe_initialize_system_user() -> PgResult<()> {
    let info = miscinit::client_connection_info();
    if let Some(authn_id) = info.authn_id.as_deref() {
        miscinit::InitializeSystemUser(
            authn_id,
            hba_authname(info.auth_method),
        );
    }
    Ok(())
}

// Keep DEBUG3 referenced (the C view-fill / include tokenizer uses it; the
// loaders use LOG, but DEBUG3 is the documented sibling level).
const _: ::types_error::ErrorLevel = DEBUG3;
