use ifaddr::AddressFamily;
use ip::{AddrInfoHint, PgAddrInfo};
use types_core::init::{
    uaBSD, uaCert, uaGSS, uaIdent, uaLDAP, uaMD5, uaOAuth, uaPAM, uaPassword, uaPeer, uaRADIUS,
    uaReject, uaSCRAM, uaSSPI, uaTrust,
};
use types_error::{ErrorLevel, PgResult};
use types_startup::{
    clientCertCA, clientCertCN, clientCertDN, clientCertFull, ctHost, ctHostGSS, ctHostNoGSS,
    ctHostNoSSL, ctHostSSL, ctLocal, ipCmpAll, ipCmpMask, ipCmpSameHost, ipCmpSameNet, HbaLine,
    LDAP_SCOPE_SUBTREE,
};

use crate::check::{ipaddr_to_sockaddr, ss_family};
use crate::ldapurl::ldap_url_parse;
use crate::token::{copy_auth_token, regcomp_auth_token};
use crate::{report_config, token_is_keyword, TokenizedAuthLine};

// Build flags of this tree: SSL on; LDAP on (in-tree client, OpenLDAP-style
// API surface: ldapurl accepted, ldapserver not mandatory); RADIUS on; GSS
// (auth) and PAM on; no SSPI / BSD. GSSAPI *encryption* (gssencmode) is not
// implemented, so hostgssenc lines parse cleanly (ENABLE_GSS build) but
// never match at check time.
pub(crate) const fn use_ssl() -> bool {
    true
}
pub(crate) const fn enable_gss() -> bool {
    true
}

fn numeric_host_hint() -> AddrInfoHint {
    AddrInfoHint {
        flags: ip::sys::AI_NUMERICHOST,
        family: ip::sys::AF_UNSPEC,
        socktype: 0,
    }
}

// C atoi (ldapport / radiusports values).
pub(crate) fn atoi(s: &str) -> i32 {
    let t = s.trim_start_matches(|c: char| c.is_ascii() && pg_string::isspace_c_locale(c as u8));
    let (sign, digits) = match t.as_bytes().first() {
        Some(b'-') => (-1i64, &t[1..]),
        Some(b'+') => (1, &t[1..]),
        _ => (1, t),
    };
    let mut v: i64 = 0;
    for b in digits.bytes().take_while(|b| b.is_ascii_digit()) {
        v = (v * 10 + (b - b'0') as i64).min(i32::MAX as i64 + 1);
    }
    (sign * v).clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

pub(crate) fn gai_strerror(errcode: i32) -> String {
    // SAFETY: gai_strerror returns a static NUL-terminated C string.
    unsafe {
        let p = ip::sys::gai_strerror(errcode);
        if p.is_null() {
            return String::new();
        }
        std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

// report_config + record into tok_line->err_msg + `return Ok(None)`.
macro_rules! parse_error {
    ($elevel:expr, $cline:expr, $tok_line:expr, $msg:expr) => {
        parse_error!($elevel, $cline, $tok_line, $msg, None)
    };
    ($elevel:expr, $cline:expr, $tok_line:expr, $msg:expr, $hint:expr) => {{
        let msg: String = $msg;
        report_config(
            $elevel,
            $cline,
            "parse_hba_line",
            msg.clone(),
            $hint,
            $tok_line.line_num,
            &$tok_line.file_name,
        )?;
        $tok_line.err_msg = Some(msg);
        return Ok(None);
    }};
}

pub fn parse_hba_line(
    tok_line: &mut TokenizedAuthLine,
    elevel: ErrorLevel,
) -> PgResult<Option<HbaLine>> {
    let line_num = tok_line.line_num;
    let file_name = tok_line.file_name.clone();

    let mut parsedline = HbaLine::new_zeroed();
    parsedline.sourcefile = file_name.clone();
    parsedline.linenumber = line_num;
    parsedline.rawline = tok_line.raw_line.clone();

    // Check the record type.
    debug_assert!(!tok_line.fields.is_empty());
    let mut field = 0usize;
    if tok_line.fields[field].len() > 1 {
        parse_error!(
            elevel,
            1345,
            tok_line,
            "multiple values specified for connection type".to_string(),
            Some("Specify exactly one connection type per line.")
        );
    }
    let ts = tok_line.fields[field][0].string.clone();
    if ts == "local" {
        parsedline.conntype = ctLocal;
    } else if ts == "host"
        || ts == "hostssl"
        || ts == "hostnossl"
        || ts == "hostgssenc"
        || ts == "hostnogssenc"
    {
        let b = ts.as_bytes();
        if b.get(4) == Some(&b's') {
            parsedline.conntype = ctHostSSL;
            // Log a warning if SSL support is not active.
            if use_ssl() && !guc_tables::vars::EnableSSL.read() {
                let msg = "hostssl record cannot match because SSL is disabled".to_string();
                report_config(
                    elevel,
                    1384,
                    "parse_hba_line",
                    msg.clone(),
                    Some("Set \"ssl = on\" in postgresql.conf."),
                    line_num,
                    &file_name,
                )?;
                tok_line.err_msg = Some(msg);
            }
        } else if b.get(4) == Some(&b'g') {
            parsedline.conntype = ctHostGSS;
            if !enable_gss() {
                let msg =
                    "hostgssenc record cannot match because GSSAPI is not supported by this build"
                        .to_string();
                report_config(elevel, 1399, "parse_hba_line", msg.clone(), None, line_num, &file_name)?;
                tok_line.err_msg = Some(msg);
            }
        } else if b.get(4) == Some(&b'n') && b.get(6) == Some(&b's') {
            parsedline.conntype = ctHostNoSSL;
        } else if b.get(4) == Some(&b'n') && b.get(6) == Some(&b'g') {
            parsedline.conntype = ctHostNoGSS;
        } else {
            parsedline.conntype = ctHost;
        }
    } else {
        parse_error!(
            elevel,
            1419,
            tok_line,
            format!("invalid connection type \"{ts}\"")
        );
    }

    // Get the databases.
    field += 1;
    if field >= tok_line.fields.len() {
        parse_error!(
            elevel,
            1430,
            tok_line,
            "end-of-line before database specification".to_string()
        );
    }
    for tc in tok_line.fields[field].clone() {
        let mut tok = copy_auth_token(&tc);
        let mut err_msg = None;
        if regcomp_auth_token(&mut tok, &file_name, line_num, &mut err_msg, elevel)? != 0 {
            tok_line.err_msg = err_msg;
            return Ok(None);
        }
        parsedline.databases.push(tok);
    }

    // Get the roles.
    field += 1;
    if field >= tok_line.fields.len() {
        parse_error!(
            elevel,
            1458,
            tok_line,
            "end-of-line before role specification".to_string()
        );
    }
    for tc in tok_line.fields[field].clone() {
        let mut tok = copy_auth_token(&tc);
        let mut err_msg = None;
        if regcomp_auth_token(&mut tok, &file_name, line_num, &mut err_msg, elevel)? != 0 {
            tok_line.err_msg = err_msg;
            return Ok(None);
        }
        parsedline.roles.push(tok);
    }

    if parsedline.conntype != ctLocal {
        // Read the IP address field (with or without CIDR netmask).
        field += 1;
        if field >= tok_line.fields.len() {
            parse_error!(
                elevel,
                1487,
                tok_line,
                "end-of-line before IP address specification".to_string()
            );
        }
        if tok_line.fields[field].len() > 1 {
            parse_error!(
                elevel,
                1496,
                tok_line,
                "multiple values specified for host address".to_string(),
                Some("Specify one address range per line.")
            );
        }
        let token = tok_line.fields[field][0].clone();

        if token_is_keyword(&token, "all") {
            parsedline.ip_cmp_method = ipCmpAll;
        } else if token_is_keyword(&token, "samehost") {
            parsedline.ip_cmp_method = ipCmpSameHost;
        } else if token_is_keyword(&token, "samenet") {
            parsedline.ip_cmp_method = ipCmpSameNet;
        } else {
            parsedline.ip_cmp_method = ipCmpMask;

            let str_full = token.string.clone();
            let (addr_part, cidr_slash): (&str, Option<&str>) = match str_full.find('/') {
                Some(pos) => (&str_full[..pos], Some(&str_full[pos + 1..])),
                None => (&str_full[..], None),
            };

            let hint = numeric_host_hint();
            let mut gai_result: Vec<PgAddrInfo> = Vec::new();
            let ret = ip::pg_getaddrinfo_all(Some(addr_part), None, &hint, &mut gai_result);
            if ret == 0 && !gai_result.is_empty() {
                parsedline.addr = gai_result[0].addr;
            } else if ret == ip::sys::EAI_NONAME {
                parsedline.hostname = Some(addr_part.to_string());
            } else {
                parse_error!(
                    elevel,
                    1550,
                    tok_line,
                    format!("invalid IP address \"{addr_part}\": {}", gai_strerror(ret))
                );
            }

            // Get the netmask.
            if let Some(cidr_bits) = cidr_slash {
                if parsedline.hostname.is_some() {
                    parse_error!(
                        elevel,
                        1567,
                        tok_line,
                        format!(
                            "specifying both host name and CIDR mask is invalid: \"{str_full}\""
                        )
                    );
                }

                let fam = match ss_family(&parsedline.addr) {
                    f if f == ip::sys::AF_INET => AddressFamily::Inet,
                    f if f == ip::sys::AF_INET6 => AddressFamily::Inet6,
                    _ => AddressFamily::Other,
                };
                match ifaddr::pg_sockaddr_cidr_mask(Some(cidr_bits), fam) {
                    Ok(mask_ip) => {
                        let mask_sa = ipaddr_to_sockaddr(&mask_ip);
                        parsedline.mask = mask_sa;
                        // C sets masklen = addrlen here.
                        parsedline.mask.salen = parsedline.addr.salen;
                    }
                    Err(_) => {
                        parse_error!(
                            elevel,
                            1578,
                            tok_line,
                            format!("invalid CIDR mask in address \"{str_full}\"")
                        );
                    }
                }
            } else if parsedline.hostname.is_none() {
                // Read the mask field.
                field += 1;
                if field >= tok_line.fields.len() {
                    parse_error!(
                        elevel,
                        1591,
                        tok_line,
                        "end-of-line before netmask specification".to_string(),
                        Some(
                            "Specify an address range in CIDR notation, or provide a separate netmask."
                        )
                    );
                }
                if tok_line.fields[field].len() > 1 {
                    parse_error!(
                        elevel,
                        1601,
                        tok_line,
                        "multiple values specified for netmask".to_string()
                    );
                }
                let mstr = tok_line.fields[field][0].string.clone();

                let hint = numeric_host_hint();
                let mut gai_result: Vec<PgAddrInfo> = Vec::new();
                let ret = ip::pg_getaddrinfo_all(Some(&mstr), None, &hint, &mut gai_result);
                if ret != 0 || gai_result.is_empty() {
                    parse_error!(
                        elevel,
                        1614,
                        tok_line,
                        format!("invalid IP mask \"{mstr}\": {}", gai_strerror(ret))
                    );
                }
                parsedline.mask = gai_result[0].addr;

                if ss_family(&parsedline.addr) != ss_family(&parsedline.mask) {
                    parse_error!(
                        elevel,
                        1628,
                        tok_line,
                        "IP address and mask do not match".to_string()
                    );
                }
            }
        }
    }

    // Get the authentication method.
    field += 1;
    if field >= tok_line.fields.len() {
        parse_error!(
            elevel,
            1644,
            tok_line,
            "end-of-line before authentication method".to_string()
        );
    }
    if tok_line.fields[field].len() > 1 {
        parse_error!(
            elevel,
            1653,
            tok_line,
            "multiple values specified for authentication type".to_string(),
            Some("Specify exactly one authentication type per line.")
        );
    }
    let ts = tok_line.fields[field][0].string.clone();

    let mut unsupauth: Option<&str> = None;
    match ts.as_str() {
        "trust" => parsedline.auth_method = uaTrust,
        "ident" => parsedline.auth_method = uaIdent,
        "peer" => parsedline.auth_method = uaPeer,
        "password" => parsedline.auth_method = uaPassword,
        "reject" => parsedline.auth_method = uaReject,
        "md5" => parsedline.auth_method = uaMD5,
        "scram-sha-256" => parsedline.auth_method = uaSCRAM,
        // cert is compiled in whenever SSL is (USE_SSL), which this build has.
        "cert" => parsedline.auth_method = uaCert,
        "ldap" => parsedline.auth_method = uaLDAP,
        "radius" => parsedline.auth_method = uaRADIUS,
        "gss" => parsedline.auth_method = uaGSS,
        "pam" => parsedline.auth_method = uaPAM,
        // Build-flag rejections, faithful to a no-SSPI/BSD C build.
        "sspi" => unsupauth = Some("sspi"),
        "bsd" => unsupauth = Some("bsd"),
        "oauth" => parsedline.auth_method = uaOAuth,
        _ => {
            parse_error!(
                elevel,
                1740,
                tok_line,
                format!("invalid authentication method \"{ts}\"")
            );
        }
    }
    let _ = (uaSSPI, uaBSD);

    if let Some(ua) = unsupauth {
        parse_error!(
            elevel,
            1750,
            tok_line,
            format!("invalid authentication method \"{ua}\": not supported by this build")
        );
    }

    // When using ident on local connections, change it to peer.
    if parsedline.conntype == ctLocal && parsedline.auth_method == uaIdent {
        parsedline.auth_method = uaPeer;
    }

    // Invalid authentication combinations.
    if parsedline.conntype == ctLocal && parsedline.auth_method == uaGSS {
        parse_error!(
            elevel,
            1790,
            tok_line,
            "gssapi authentication is not supported on local sockets".to_string()
        );
    }

    if parsedline.conntype != ctLocal && parsedline.auth_method == uaPeer {
        parse_error!(
            elevel,
            1774,
            tok_line,
            "peer authentication is only supported on local sockets".to_string()
        );
    }

    // SSPI authentication can never be enabled on ctLocal connections,
    // because it's only supported on Windows, where ctLocal isn't supported.

    if parsedline.conntype != ctHostSSL && parsedline.auth_method == uaCert {
        parse_error!(
            elevel,
            1822,
            tok_line,
            "cert authentication is only supported on hostssl connections".to_string()
        );
    }

    // GSS/SSPI include_realm defaults to true (multi-realm safety).
    if parsedline.auth_method == uaGSS {
        parsedline.include_realm = true;
    }

    // Parse remaining arguments.
    field += 1;
    while field < tok_line.fields.len() {
        for token in tok_line.fields[field].clone() {
            let raw = token.string.clone();
            let Some(pos) = raw.find('=') else {
                parse_error!(
                    elevel,
                    1875,
                    tok_line,
                    format!("authentication option not in name=value format: {raw}")
                );
            };
            let name = raw[..pos].to_string();
            let val = raw[pos + 1..].to_string();
            let mut err_msg = None;
            let ok = parse_hba_auth_opt(&name, &val, &mut parsedline, elevel, &mut err_msg)?;
            if let Some(e) = err_msg {
                tok_line.err_msg = Some(e);
            }
            if !ok {
                return Ok(None);
            }
        }
        field += 1;
    }

    // Check if the selected authentication method has any mandatory
    // arguments that are not set. (oauth is unreachable in this build.)
    if parsedline.auth_method == uaLDAP {
        // Not mandatory: ldapserver may be omitted (HAVE_LDAP_INITIALIZE
        // surface; the DNS SRV lookup itself is unimplemented and fails at
        // connection time — see auth ldap module).
        if parsedline.ldapprefix.is_some() || parsedline.ldapsuffix.is_some() {
            if parsedline.ldapbasedn.is_some()
                || parsedline.ldapbinddn.is_some()
                || parsedline.ldapbindpasswd.is_some()
                || parsedline.ldapsearchattribute.is_some()
                || parsedline.ldapsearchfilter.is_some()
            {
                parse_error!(
                    elevel,
                    1917,
                    tok_line,
                    "cannot mix options for simple bind and search+bind modes".to_string()
                );
            }
        } else if parsedline.ldapbasedn.is_none() {
            parse_error!(
                elevel,
                1928,
                tok_line,
                "authentication method \"ldap\" requires argument \"ldapbasedn\", \"ldapprefix\", or \"ldapsuffix\" to be set"
                    .to_string()
            );
        }

        if parsedline.ldapsearchattribute.is_some() && parsedline.ldapsearchfilter.is_some() {
            parse_error!(
                elevel,
                1943,
                tok_line,
                "cannot use ldapsearchattribute together with ldapsearchfilter".to_string()
            );
        }
    }

    if parsedline.auth_method == uaRADIUS {
        // MANDATORY_AUTH_ARG tests the parsed List, which is NIL both when
        // the option was absent and when its value was an empty list, so the
        // "list of RADIUS servers/secrets cannot be empty" arms are dead
        // code in C and unrepresented here.
        if parsedline.radiusservers.is_empty() {
            parse_error!(
                elevel,
                1955,
                tok_line,
                "authentication method \"radius\" requires argument \"radiusservers\" to be set"
                    .to_string()
            );
        }
        if parsedline.radiussecrets.is_empty() {
            parse_error!(
                elevel,
                1956,
                tok_line,
                "authentication method \"radius\" requires argument \"radiussecrets\" to be set"
                    .to_string()
            );
        }
        // Each option list must have length 0 (secrets excepted, checked
        // above), 1, or the same as the number of servers.
        let nservers = parsedline.radiusservers.len();
        let nsecrets = parsedline.radiussecrets.len();
        if !(nsecrets == 1 || nsecrets == nservers) {
            parse_error!(
                elevel,
                1988,
                tok_line,
                format!(
                    "the number of RADIUS secrets ({nsecrets}) must be 1 or the same as the number of RADIUS servers ({nservers})"
                )
            );
        }
        let nports = parsedline.radiusports.len();
        if !(nports == 0 || nports == 1 || nports == nservers) {
            parse_error!(
                elevel,
                2004,
                tok_line,
                format!(
                    "the number of RADIUS ports ({nports}) must be 1 or the same as the number of RADIUS servers ({nservers})"
                )
            );
        }
        let nident = parsedline.radiusidentifiers.len();
        if !(nident == 0 || nident == 1 || nident == nservers) {
            parse_error!(
                elevel,
                2020,
                tok_line,
                format!(
                    "the number of RADIUS identifiers ({nident}) must be 1 or the same as the number of RADIUS servers ({nservers})"
                )
            );
        }
    }

    // Enforce proper configuration of OAuth authentication.
    if parsedline.auth_method == uaOAuth {
        // MANDATORY_AUTH_ARG(oauth_scope, "scope", "oauth")
        if parsedline.oauth_scope.is_none() {
            parse_error!(
                elevel,
                2051,
                tok_line,
                "authentication method \"oauth\" requires argument \"scope\" to be set".to_string()
            );
        }
        if parsedline.oauth_issuer.is_none() {
            parse_error!(
                elevel,
                2052,
                tok_line,
                "authentication method \"oauth\" requires argument \"issuer\" to be set".to_string()
            );
        }

        // Ensure a validator library is set and permitted by the config.
        if let Some(msg) = check_oauth_validator(&mut parsedline, elevel)? {
            tok_line.err_msg = Some(msg);
            return Ok(None);
        }

        // Supplying a usermap combined with the option to skip usermapping is
        // nonsensical and indicates a configuration error.
        if parsedline.oauth_skip_usermap && parsedline.usermap.is_some() {
            parse_error!(
                elevel,
                2066,
                tok_line,
                "map cannot be used in combination with delegate_ident_mapping".to_string()
            );
        }
    }

    // Enforce any parameters implied by other settings.
    if parsedline.auth_method == uaCert {
        // For auth method cert, client certificate validation is mandatory,
        // and it implies the level of verify-full.
        parsedline.clientcert = clientCertFull;
    }

    Ok(Some(parsedline))
}

// check_oauth_validator (auth-oauth.c:819): the validator named in the HBA
// must be permitted by oauth_validator_libraries; with the option unset and
// exactly one library listed, that library is the validator. Returns
// Some(err_msg) on failure (C's *err_msg + false).
fn check_oauth_validator(
    hbaline: &mut HbaLine,
    elevel: ErrorLevel,
) -> PgResult<Option<String>> {
    use types_error::{ERRCODE_CONFIG_FILE_ERROR, ERRCODE_INVALID_PARAMETER_VALUE};

    let line_num = hbaline.linenumber;
    let file_name = hbaline.sourcefile.clone();
    let oloc = |line: i32| {
        types_error::ErrorLocation::new(
            "src/backend/libpq/auth-oauth.c",
            line,
            "check_oauth_validator",
        )
    };
    let errcontext = |line_num: i32, file_name: &str| {
        format!("line {line_num} of configuration file \"{file_name}\"")
    };

    let libraries = guc_tables::vars::oauth_validator_libraries_string
        .read()
        .unwrap_or_default();

    if libraries.is_empty() {
        let msg =
            "oauth_validator_libraries must be set for authentication method oauth".to_string();
        elog::ereport(elevel)
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(msg.clone())
            .errcontext_msg(errcontext(line_num, &file_name))
            .finish(oloc(831))?;
        return Ok(Some(msg));
    }

    let elemlist: Vec<String> = match pg_string::split_directories_string(&libraries, b',') {
        Ok(list) => list.iter().map(|s| pg_path::canonicalize_path(s)).collect(),
        Err(()) => {
            // syntax error in list
            let msg =
                "invalid list syntax in parameter \"oauth_validator_libraries\"".to_string();
            elog::ereport(elevel)
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(msg.clone())
                .finish(oloc(848))?;
            return Ok(Some(msg));
        }
    };

    let Some(validator) = &hbaline.oauth_validator else {
        if elemlist.len() == 1 {
            hbaline.oauth_validator = Some(elemlist[0].clone());
            return Ok(None);
        }
        let msg = "authentication method \"oauth\" requires argument \"validator\" to be set when oauth_validator_libraries contains multiple options".to_string();
        elog::ereport(elevel)
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(msg.clone())
            .errcontext_msg(errcontext(line_num, &file_name))
            .finish(oloc(865))?;
        return Ok(Some(msg));
    };

    if elemlist.iter().any(|allowed| allowed == validator) {
        return Ok(None);
    }

    let msg =
        format!("validator \"{validator}\" is not permitted by oauth_validator_libraries");
    elog::ereport(elevel)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(msg.clone())
        .errcontext_msg(errcontext(line_num, &file_name))
        .finish(oloc(880))?;
    Ok(Some(msg))
}

pub(crate) fn parse_hba_auth_opt(
    name: &str,
    val: &str,
    hbaline: &mut HbaLine,
    elevel: ErrorLevel,
    err_msg: &mut Option<String>,
) -> PgResult<bool> {
    let line_num = hbaline.linenumber;
    let file_name = hbaline.sourcefile.clone();

    macro_rules! opt_error {
        ($cline:expr, $msg:expr) => {{
            let msg: String = $msg;
            report_config(
                elevel,
                $cline,
                "parse_hba_auth_opt",
                msg.clone(),
                None,
                line_num,
                &file_name,
            )?;
            *err_msg = Some(msg);
            return Ok(false);
        }};
    }

    // INVALID_AUTH_OPTION(optname, validmethods).
    macro_rules! invalid_auth_option {
        ($optname:expr, $validmethods:expr) => {{
            opt_error!(
                2096,
                format!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    $optname, $validmethods
                )
            );
        }};
    }
    macro_rules! require_auth_option {
        ($methodval:expr, $optname:expr, $validmethods:expr) => {{
            if hbaline.auth_method != $methodval {
                invalid_auth_option!($optname, $validmethods);
            }
        }};
    }

    // C quirk (hba.c:2094): under USE_LDAP the scope is re-defaulted to
    // SUBTREE on every option parsed, so an ldapurl-supplied scope is
    // clobbered by any option that follows it on the line.
    hbaline.ldapscope = LDAP_SCOPE_SUBTREE;

    match name {
        "map" => {
            // valid for ident/peer/gss/sspi/cert/oauth; only ident, peer,
            // ldap, radius, pam, gss, cert, and oauth survive method parse in this build.
            if hbaline.auth_method != types_core::init::uaIdent
                && hbaline.auth_method != types_core::init::uaPeer
                && hbaline.auth_method != types_core::init::uaGSS
                && hbaline.auth_method != types_core::init::uaCert
                && hbaline.auth_method != uaOAuth
            {
                invalid_auth_option!("map", "ident, peer, gssapi, sspi, cert, and oauth");
            }
            hbaline.usermap = Some(val.to_string());
        }
        "clientcert" => {
            if hbaline.conntype != ctHostSSL {
                opt_error!(
                    2126,
                    "clientcert can only be configured for \"hostssl\" rows".to_string()
                );
            }
            if val == "verify-full" {
                hbaline.clientcert = clientCertFull;
            } else if val == "verify-ca" {
                if hbaline.auth_method == types_core::init::uaCert {
                    // C's ereport and *err_msg wordings differ at this site;
                    // keep both faithful.
                    report_config(
                        elevel,
                        2129,
                        "parse_hba_auth_opt",
                        "clientcert only accepts \"verify-full\" when using \"cert\" authentication"
                            .to_string(),
                        None,
                        line_num,
                        &file_name,
                    )?;
                    *err_msg = Some(
                        "clientcert can only be set to \"verify-full\" when using \"cert\" authentication"
                            .to_string(),
                    );
                    return Ok(false);
                }
                hbaline.clientcert = clientCertCA;
            } else {
                report_config(
                    elevel,
                    2150,
                    "parse_hba_auth_opt",
                    format!("invalid value for clientcert: \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            }
        }
        "clientname" => {
            if hbaline.conntype != ctHostSSL {
                opt_error!(
                    2162,
                    "clientname can only be configured for \"hostssl\" rows".to_string()
                );
            }
            if val == "CN" {
                hbaline.clientcertname = clientCertCN;
            } else if val == "DN" {
                hbaline.clientcertname = clientCertDN;
            } else {
                report_config(
                    elevel,
                    2181,
                    "parse_hba_auth_opt",
                    format!("invalid value for clientname: \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            }
        }
        "ldapurl" => {
            require_auth_option!(uaLDAP, "ldapurl", "ldap");
            let urldata = match ldap_url_parse(val) {
                Ok(u) => u,
                Err(rc) => {
                    opt_error!(
                        2208,
                        format!(
                            "could not parse LDAP URL \"{val}\": {}",
                            crate::ldapurl::ldap_err2string(rc)
                        )
                    );
                }
            };
            if urldata.scheme != "ldap" && urldata.scheme != "ldaps" {
                opt_error!(
                    2219,
                    format!("unsupported LDAP URL scheme: {}", urldata.scheme)
                );
            }
            hbaline.ldapscheme = Some(urldata.scheme);
            if let Some(host) = urldata.host {
                hbaline.ldapserver = Some(host);
            }
            hbaline.ldapport = urldata.port;
            if let Some(dn) = urldata.dn {
                hbaline.ldapbasedn = Some(dn);
            }
            if let Some(attr) = urldata.attrs.into_iter().next() {
                // only use first one
                hbaline.ldapsearchattribute = Some(attr);
            }
            hbaline.ldapscope = urldata.scope;
            if let Some(filter) = urldata.filter {
                hbaline.ldapsearchfilter = Some(filter);
            }
        }
        "ldaptls" => {
            require_auth_option!(uaLDAP, "ldaptls", "ldap");
            hbaline.ldaptls = val == "1";
        }
        "ldapscheme" => {
            require_auth_option!(uaLDAP, "ldapscheme", "ldap");
            if val != "ldap" && val != "ldaps" {
                // C reports but does not fail: the value is stored anyway.
                report_config(
                    elevel,
                    2261,
                    "parse_hba_auth_opt",
                    format!("invalid ldapscheme value: \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
            }
            hbaline.ldapscheme = Some(val.to_string());
        }
        "ldapserver" => {
            require_auth_option!(uaLDAP, "ldapserver", "ldap");
            hbaline.ldapserver = Some(val.to_string());
        }
        "ldapport" => {
            require_auth_option!(uaLDAP, "ldapport", "ldap");
            hbaline.ldapport = atoi(val);
            if hbaline.ldapport == 0 {
                opt_error!(2276, format!("invalid LDAP port number: \"{val}\""));
            }
        }
        "ldapbinddn" => {
            require_auth_option!(uaLDAP, "ldapbinddn", "ldap");
            hbaline.ldapbinddn = Some(val.to_string());
        }
        "ldapbindpasswd" => {
            require_auth_option!(uaLDAP, "ldapbindpasswd", "ldap");
            hbaline.ldapbindpasswd = Some(val.to_string());
        }
        "ldapsearchattribute" => {
            require_auth_option!(uaLDAP, "ldapsearchattribute", "ldap");
            hbaline.ldapsearchattribute = Some(val.to_string());
        }
        "ldapsearchfilter" => {
            require_auth_option!(uaLDAP, "ldapsearchfilter", "ldap");
            hbaline.ldapsearchfilter = Some(val.to_string());
        }
        "ldapbasedn" => {
            require_auth_option!(uaLDAP, "ldapbasedn", "ldap");
            hbaline.ldapbasedn = Some(val.to_string());
        }
        "ldapprefix" => {
            require_auth_option!(uaLDAP, "ldapprefix", "ldap");
            hbaline.ldapprefix = Some(val.to_string());
        }
        "ldapsuffix" => {
            require_auth_option!(uaLDAP, "ldapsuffix", "ldap");
            hbaline.ldapsuffix = Some(val.to_string());
        }
        "pamservice" => {
            if hbaline.auth_method != types_core::init::uaPAM {
                invalid_auth_option!("pamservice", "pam");
            }
            hbaline.pamservice = Some(val.to_string());
        }
        "pam_use_hostname" => {
            if hbaline.auth_method != types_core::init::uaPAM {
                invalid_auth_option!("pam_use_hostname", "pam");
            }
            hbaline.pam_use_hostname = val == "1";
        }
        // NB: the ldap* options are handled in full by the arms above, whose
        // require_auth_option!(uaLDAP, ...) already emits the identical
        // invalid_auth_option!(opt, "ldap") when the method is not LDAP. The
        // duplicate reject-only arms that used to live here (a leftover from
        // before the LDAP leg was ported) were dead code — a newer rustc flags
        // them as unreachable and the lint gate rejects them tree-wide.
        // gssapi and sspi in C; sspi is rejected at method parse here.
        "krb_realm" => {
            if hbaline.auth_method != types_core::init::uaGSS {
                invalid_auth_option!("krb_realm", "gssapi and sspi");
            }
            hbaline.krb_realm = Some(val.to_string());
        }
        "include_realm" => {
            if hbaline.auth_method != types_core::init::uaGSS {
                invalid_auth_option!("include_realm", "gssapi and sspi");
            }
            hbaline.include_realm = val == "1";
        }
        "compat_realm" => invalid_auth_option!("compat_realm", "sspi"),
        "upn_username" => invalid_auth_option!("upn_username", "sspi"),
        "radiusservers" => {
            require_auth_option!(uaRADIUS, "radiusservers", "radius");
            let Ok(parsed_servers) = pg_string::split_guc_list(val, b',') else {
                // C reports without setting *err_msg here.
                report_config(
                    elevel,
                    2354,
                    "parse_hba_auth_opt",
                    format!("could not parse RADIUS server list \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            };
            // For each entry in the list, translate it.
            for server in &parsed_servers {
                let hint = AddrInfoHint {
                    flags: 0,
                    family: ip::sys::AF_UNSPEC,
                    socktype: libc::SOCK_DGRAM,
                };
                let mut gai_result: Vec<PgAddrInfo> = Vec::new();
                let ret = ip::pg_getaddrinfo_all(Some(server), None, &hint, &mut gai_result);
                if ret != 0 || gai_result.is_empty() {
                    report_config(
                        elevel,
                        2373,
                        "parse_hba_auth_opt",
                        format!(
                            "could not translate RADIUS server name \"{server}\" to address: {}",
                            gai_strerror(ret)
                        ),
                        None,
                        line_num,
                        &file_name,
                    )?;
                    return Ok(false);
                }
            }
            hbaline.radiusservers = parsed_servers;
            hbaline.radiusservers_s = Some(val.to_string());
        }
        "radiusports" => {
            require_auth_option!(uaRADIUS, "radiusports", "radius");
            let Ok(parsed_ports) = pg_string::split_guc_list(val, b',') else {
                // C quirk: the report and *err_msg wordings differ here.
                report_config(
                    elevel,
                    2420,
                    "parse_hba_auth_opt",
                    format!("could not parse RADIUS port list \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                *err_msg = Some(format!("invalid RADIUS port number: \"{val}\""));
                return Ok(false);
            };
            for port in &parsed_ports {
                if atoi(port) == 0 {
                    // C reports without setting *err_msg here.
                    report_config(
                        elevel,
                        2434,
                        "parse_hba_auth_opt",
                        format!("invalid RADIUS port number: \"{val}\""),
                        None,
                        line_num,
                        &file_name,
                    )?;
                    return Ok(false);
                }
            }
            hbaline.radiusports = parsed_ports;
            hbaline.radiusports_s = Some(val.to_string());
        }
        "radiussecrets" => {
            require_auth_option!(uaRADIUS, "radiussecrets", "radius");
            let Ok(parsed_secrets) = pg_string::split_guc_list(val, b',') else {
                report_config(
                    elevel,
                    2456,
                    "parse_hba_auth_opt",
                    format!("could not parse RADIUS secret list \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            };
            hbaline.radiussecrets = parsed_secrets;
            hbaline.radiussecrets_s = Some(val.to_string());
        }
        "radiusidentifiers" => {
            require_auth_option!(uaRADIUS, "radiusidentifiers", "radius");
            let Ok(parsed_identifiers) = pg_string::split_guc_list(val, b',') else {
                report_config(
                    elevel,
                    2478,
                    "parse_hba_auth_opt",
                    format!("could not parse RADIUS identifiers list \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            };
            hbaline.radiusidentifiers = parsed_identifiers;
            hbaline.radiusidentifiers_s = Some(val.to_string());
        }
        // REQUIRE_AUTH_OPTION(uaOAuth, ...) arms (hba.c:2488-2510).
        "issuer" => {
            if hbaline.auth_method != uaOAuth {
                invalid_auth_option!("issuer", "oauth");
            }
            hbaline.oauth_issuer = Some(val.to_string());
        }
        "scope" => {
            if hbaline.auth_method != uaOAuth {
                invalid_auth_option!("scope", "oauth");
            }
            hbaline.oauth_scope = Some(val.to_string());
        }
        "validator" => {
            if hbaline.auth_method != uaOAuth {
                invalid_auth_option!("validator", "oauth");
            }
            hbaline.oauth_validator = Some(val.to_string());
        }
        "delegate_ident_mapping" => {
            if hbaline.auth_method != uaOAuth {
                invalid_auth_option!("delegate_ident_mapping", "oauth");
            }
            hbaline.oauth_skip_usermap = val == "1";
        }
        _ => {
            opt_error!(
                2517,
                format!("unrecognized authentication option name: \"{name}\"")
            );
        }
    }
    Ok(true)
}
