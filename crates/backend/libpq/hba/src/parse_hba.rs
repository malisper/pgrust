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
};

use crate::check::{ipaddr_to_sockaddr, ss_family};
use crate::token::{copy_auth_token, regcomp_auth_token};
use crate::{report_config, token_is_keyword, TokenizedAuthLine};

// Build flags of this tree: SSL on; no GSS / SSPI / PAM / BSD / LDAP.
pub(crate) const fn use_ssl() -> bool {
    true
}
pub(crate) const fn enable_gss() -> bool {
    false
}

fn numeric_host_hint() -> AddrInfoHint {
    AddrInfoHint {
        flags: ip::sys::AI_NUMERICHOST,
        family: ip::sys::AF_UNSPEC,
        socktype: 0,
    }
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
        // Build-flag rejections, faithful to a no-GSS/SSPI/PAM/BSD/LDAP C build.
        "gss" => unsupauth = Some("gss"),
        "sspi" => unsupauth = Some("sspi"),
        "pam" => unsupauth = Some("pam"),
        "bsd" => unsupauth = Some("bsd"),
        "ldap" => unsupauth = Some("ldap"),
        // C accepts these in every build; their handlers are unported.
        "radius" | "oauth" => panic!(
            "parse_hba_line: auth method \"{ts}\" ({file_name} line {line_num}) — \
             backend-libpq-auth {ts} arm unported"
        ),
        _ => {
            parse_error!(
                elevel,
                1740,
                tok_line,
                format!("invalid authentication method \"{ts}\"")
            );
        }
    }
    let _ = (uaGSS, uaSSPI, uaPAM, uaBSD, uaLDAP, uaRADIUS, uaOAuth);

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

    if parsedline.conntype != ctLocal && parsedline.auth_method == uaPeer {
        parse_error!(
            elevel,
            1774,
            tok_line,
            "peer authentication is only supported on local sockets".to_string()
        );
    }

    // (local+gss combination check: gss is rejected at method parse in this
    // build.)

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

    // (Per-method mandatory-argument checks: ldap / radius / oauth are
    // unreachable in this build.)

    // Enforce any parameters implied by other settings.
    if parsedline.auth_method == uaCert {
        // For auth method cert, client certificate validation is mandatory,
        // and it implies the level of verify-full.
        parsedline.clientcert = clientCertFull;
    }

    Ok(Some(parsedline))
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

    // INVALID_AUTH_OPTION(optname, validmethods) — in this build every method
    // these options require is rejected at method parse, so the invalid arm
    // is the only reachable one.
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

    match name {
        "map" => {
            // valid for ident/peer/gss/sspi/cert/oauth; only ident, peer, and
            // cert survive method parse in this build.
            if hbaline.auth_method != types_core::init::uaIdent
                && hbaline.auth_method != types_core::init::uaPeer
                && hbaline.auth_method != types_core::init::uaCert
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
        "pamservice" => invalid_auth_option!("pamservice", "pam"),
        "pam_use_hostname" => invalid_auth_option!("pam_use_hostname", "pam"),
        "ldapurl" => invalid_auth_option!("ldapurl", "ldap"),
        "ldaptls" => invalid_auth_option!("ldaptls", "ldap"),
        "ldapscheme" => invalid_auth_option!("ldapscheme", "ldap"),
        "ldapserver" => invalid_auth_option!("ldapserver", "ldap"),
        "ldapport" => invalid_auth_option!("ldapport", "ldap"),
        "ldapbinddn" => invalid_auth_option!("ldapbinddn", "ldap"),
        "ldapbindpasswd" => invalid_auth_option!("ldapbindpasswd", "ldap"),
        "ldapsearchattribute" => invalid_auth_option!("ldapsearchattribute", "ldap"),
        "ldapsearchfilter" => invalid_auth_option!("ldapsearchfilter", "ldap"),
        "ldapbasedn" => invalid_auth_option!("ldapbasedn", "ldap"),
        "ldapprefix" => invalid_auth_option!("ldapprefix", "ldap"),
        "ldapsuffix" => invalid_auth_option!("ldapsuffix", "ldap"),
        "krb_realm" => invalid_auth_option!("krb_realm", "gssapi and sspi"),
        "include_realm" => invalid_auth_option!("include_realm", "gssapi and sspi"),
        "compat_realm" => invalid_auth_option!("compat_realm", "sspi"),
        "upn_username" => invalid_auth_option!("upn_username", "sspi"),
        "radiusservers" => invalid_auth_option!("radiusservers", "radius"),
        "radiussecrets" => invalid_auth_option!("radiussecrets", "radius"),
        "radiusidentifiers" => invalid_auth_option!("radiusidentifiers", "radius"),
        "radiusports" => invalid_auth_option!("radiusports", "radius"),
        "issuer" => invalid_auth_option!("issuer", "oauth"),
        "scope" => invalid_auth_option!("scope", "oauth"),
        "validator" => invalid_auth_option!("validator", "oauth"),
        "delegate_ident_mapping" => invalid_auth_option!("delegate_ident_mapping", "oauth"),
        _ => {
            opt_error!(
                2517,
                format!("unrecognized authentication option name: \"{name}\"")
            );
        }
    }
    Ok(true)
}
