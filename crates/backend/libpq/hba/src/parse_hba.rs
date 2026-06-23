//! The `pg_hba.conf` line parser: `parse_hba_line` (one tokenized line ->
//! `HbaLine`) and `parse_hba_auth_opt` (one `name=value` auth option).
//!
//! Ported from `src/backend/libpq/hba.c` (lines 1327-2524).

use ::ifaddr::AddressFamily;
use types_error::{ErrorLevel, PgResult};
use net::{
    clientCertCA, clientCertCN, clientCertDN, clientCertFull, ctHost, ctHostGSS, ctHostNoGSS,
    ctHostNoSSL, ctHostSSL, ctLocal, ipCmpAll, ipCmpMask, ipCmpSameHost, ipCmpSameNet, HbaLine,
};
use types_core::{
    uaBSD, uaCert, uaGSS, uaIdent, uaLDAP, uaMD5, uaOAuth, uaPAM, uaPassword, uaPeer, uaRADIUS,
    uaReject, uaSCRAM, uaSSPI, uaTrust,
};

use crate::matchers::{ipaddr_to_sockaddr, ss_family};
use crate::token::{copy_auth_token, regcomp_auth_token};
use crate::{
    enable_gss, enable_sspi, have_ldap_initialize, ldap_api_feature_x_openldap, line_context, report_config,
    tok_str, token_is_keyword, use_bsd_auth, use_ldap, use_pam, use_ssl, MemCtx, TokenizedAuthLine,
};

/// Make a numeric-host `getaddrinfo` (AI_NUMERICHOST, AF_UNSPEC) hint.
fn numeric_host_hint() -> ::net::AddrInfoHint {
    ::net::AddrInfoHint {
        flags: libc::AI_NUMERICHOST,
        family: libc::AF_UNSPEC,
        socktype: 0,
    }
}

/// `gai_strerror(errcode)`.
fn gai_strerror(errcode: i32) -> String {
    // SAFETY: gai_strerror returns a static NUL-terminated C string.
    unsafe {
        let p = libc::gai_strerror(errcode);
        if p.is_null() {
            return String::new();
        }
        std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

/// `HbaLine *parse_hba_line(TokenizedAuthLine *tok_line, int elevel)`
/// (hba.c:1327). Parse one tokenized line into an [`HbaLine`]; `None` on error
/// (with `tok_line.err_msg` set).
pub fn parse_hba_line(
    tok_line: &mut TokenizedAuthLine,
    elevel: ErrorLevel,
) -> PgResult<Option<HbaLine>> {
    let line_num = tok_line.line_num;
    let file_name = tok_line.file_name.clone();

    let mut parsedline = HbaLine::new_zeroed();
    parsedline.sourcefile = Some(file_name.clone());
    parsedline.linenumber = line_num;
    parsedline.rawline = Some(tok_line.raw_line.clone());

    // Check the record type.
    debug_assert!(!tok_line.fields.is_empty());
    let mut field = 0usize;
    let tokens = &tok_line.fields[field];
    if tokens.len() > 1 {
        report_config(
            elevel,
            "parse_hba_line",
            "multiple values specified for connection type".to_string(),
            Some("Specify exactly one connection type per line."),
            line_num,
            &file_name,
        )?;
        tok_line.err_msg = Some("multiple values specified for connection type".to_string());
        return Ok(None);
    }
    let token = &tokens[0];
    let ts = tok_str(token).to_vec();
    if ts == b"local" {
        parsedline.conntype = ctLocal;
    } else if ts == b"host"
        || ts == b"hostssl"
        || ts == b"hostnossl"
        || ts == b"hostgssenc"
        || ts == b"hostnogssenc"
    {
        if ts.get(4) == Some(&b's') {
            // "hostssl"
            parsedline.conntype = ctHostSSL;
            // Log a warning if SSL support is not active.
            if use_ssl() {
                if !crate::enable_ssl() {
                    report_config(
                        elevel,
                        "parse_hba_line",
                        "hostssl record cannot match because SSL is disabled".to_string(),
                        Some("Set \"ssl = on\" in postgresql.conf."),
                        line_num,
                        &file_name,
                    )?;
                    tok_line.err_msg =
                        Some("hostssl record cannot match because SSL is disabled".to_string());
                }
            } else {
                report_config(
                    elevel,
                    "parse_hba_line",
                    "hostssl record cannot match because SSL is not supported by this build"
                        .to_string(),
                    None,
                    line_num,
                    &file_name,
                )?;
                tok_line.err_msg = Some(
                    "hostssl record cannot match because SSL is not supported by this build"
                        .to_string(),
                );
            }
        } else if ts.get(4) == Some(&b'g') {
            // "hostgssenc"
            parsedline.conntype = ctHostGSS;
            if !enable_gss() {
                report_config(
                    elevel,
                    "parse_hba_line",
                    "hostgssenc record cannot match because GSSAPI is not supported by this build"
                        .to_string(),
                    None,
                    line_num,
                    &file_name,
                )?;
                tok_line.err_msg = Some(
                    "hostgssenc record cannot match because GSSAPI is not supported by this build"
                        .to_string(),
                );
            }
        } else if ts.get(4) == Some(&b'n') && ts.get(6) == Some(&b's') {
            parsedline.conntype = ctHostNoSSL;
        } else if ts.get(4) == Some(&b'n') && ts.get(6) == Some(&b'g') {
            parsedline.conntype = ctHostNoGSS;
        } else {
            // "host"
            parsedline.conntype = ctHost;
        }
    } else {
        let s = String::from_utf8_lossy(&ts);
        report_config(
            elevel,
            "parse_hba_line",
            format!("invalid connection type \"{s}\""),
            None,
            line_num,
            &file_name,
        )?;
        tok_line.err_msg = Some(format!("invalid connection type \"{s}\""));
        return Ok(None);
    }

    // Get the databases.
    field += 1;
    if field >= tok_line.fields.len() {
        report_config(
            elevel,
            "parse_hba_line",
            "end-of-line before database specification".to_string(),
            None,
            line_num,
            &file_name,
        )?;
        tok_line.err_msg = Some("end-of-line before database specification".to_string());
        return Ok(None);
    }
    parsedline.databases = Vec::new();
    {
        let tokens = tok_line.fields[field].clone();
        for tc in &tokens {
            let mut tok = copy_auth_token(tc);
            // Compile a regexp for the database token, if necessary.
            if regcomp_auth_token(&mut tok, &file_name, line_num, &mut tok_line.err_msg, elevel)? != 0
            {
                return Ok(None);
            }
            parsedline.databases.push(tok);
        }
    }

    // Get the roles.
    field += 1;
    if field >= tok_line.fields.len() {
        report_config(
            elevel,
            "parse_hba_line",
            "end-of-line before role specification".to_string(),
            None,
            line_num,
            &file_name,
        )?;
        tok_line.err_msg = Some("end-of-line before role specification".to_string());
        return Ok(None);
    }
    parsedline.roles = Vec::new();
    {
        let tokens = tok_line.fields[field].clone();
        for tc in &tokens {
            let mut tok = copy_auth_token(tc);
            if regcomp_auth_token(&mut tok, &file_name, line_num, &mut tok_line.err_msg, elevel)? != 0
            {
                return Ok(None);
            }
            parsedline.roles.push(tok);
        }
    }

    if parsedline.conntype != ctLocal {
        // Read the IP address field (with or without CIDR netmask).
        field += 1;
        if field >= tok_line.fields.len() {
            report_config(
                elevel,
                "parse_hba_line",
                "end-of-line before IP address specification".to_string(),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some("end-of-line before IP address specification".to_string());
            return Ok(None);
        }
        let tokens = tok_line.fields[field].clone();
        if tokens.len() > 1 {
            report_config(
                elevel,
                "parse_hba_line",
                "multiple values specified for host address".to_string(),
                Some("Specify one address range per line."),
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some("multiple values specified for host address".to_string());
            return Ok(None);
        }
        let token = &tokens[0];

        if token_is_keyword(token, b"all") {
            parsedline.ip_cmp_method = ipCmpAll;
        } else if token_is_keyword(token, b"samehost") {
            // Any IP on this host is allowed to connect.
            parsedline.ip_cmp_method = ipCmpSameHost;
        } else if token_is_keyword(token, b"samenet") {
            // Any IP on the host's subnets is allowed to connect.
            parsedline.ip_cmp_method = ipCmpSameNet;
        } else {
            // IP and netmask are specified.
            parsedline.ip_cmp_method = ipCmpMask;

            // need a modifiable copy of token.
            let str_full = String::from_utf8_lossy(tok_str(token)).into_owned();
            // Check if it has a CIDR suffix and if so isolate it.
            let (addr_part, cidr_slash): (&str, Option<&str>) = match str_full.find('/') {
                Some(pos) => (&str_full[..pos], Some(&str_full[pos + 1..])),
                None => (&str_full[..], None),
            };

            // Get the IP address either way.
            let hint = numeric_host_hint();
            let mcx = MemCtx::new("parse_hba_line/addr");
            let _ = mcx; // ip allocates into Vec, no mcx needed
            let mut gai_result: Vec<::net::PgAddrInfo> = Vec::new();
            let ret =
                ip::pg_getaddrinfo_all(Some(addr_part), None, &hint, &mut gai_result);
            if ret == 0 && !gai_result.is_empty() {
                // memcpy(&parsedline->addr, gai_result->ai_addr, ai_addrlen);
                parsedline.addr = gai_result[0].addr.addr;
                parsedline.addrlen = gai_result[0].addr.salen as i32;
            } else if ret == crate::EAI_NONAME {
                parsedline.hostname = Some(addr_part.to_string());
            } else {
                let s = String::from_utf8_lossy(tok_str(token));
                let _ = s;
                report_config(
                    elevel,
                    "parse_hba_line",
                    format!("invalid IP address \"{addr_part}\": {}", gai_strerror(ret)),
                    None,
                    line_num,
                    &file_name,
                )?;
                tok_line.err_msg = Some(format!(
                    "invalid IP address \"{addr_part}\": {}",
                    gai_strerror(ret)
                ));
                return Ok(None);
            }

            // Get the netmask.
            if let Some(cidr_bits) = cidr_slash {
                if parsedline.hostname.is_some() {
                    let full = String::from_utf8_lossy(tok_str(token));
                    report_config(
                        elevel,
                        "parse_hba_line",
                        format!("specifying both host name and CIDR mask is invalid: \"{full}\""),
                        None,
                        line_num,
                        &file_name,
                    )?;
                    tok_line.err_msg = Some(format!(
                        "specifying both host name and CIDR mask is invalid: \"{full}\""
                    ));
                    return Ok(None);
                }

                // pg_sockaddr_cidr_mask(&parsedline->mask, cidr_slash+1, addr.ss_family)
                let fam = match ss_family(&sockaddr_view(&parsedline)) {
                    f if f == libc::AF_INET => AddressFamily::Inet,
                    f if f == libc::AF_INET6 => AddressFamily::Inet6,
                    _ => AddressFamily::Other,
                };
                match ::ifaddr::pg_sockaddr_cidr_mask(Some(cidr_bits), fam) {
                    Ok(mask_ip) => {
                        let mask_sa = ipaddr_to_sockaddr(&mask_ip);
                        parsedline.mask = mask_sa.addr;
                        // masklen = addrlen (C sets masklen = addrlen here)
                        parsedline.masklen = parsedline.addrlen;
                    }
                    Err(_) => {
                        let full = String::from_utf8_lossy(tok_str(token));
                        report_config(
                            elevel,
                            "parse_hba_line",
                            format!("invalid CIDR mask in address \"{full}\""),
                            None,
                            line_num,
                            &file_name,
                        )?;
                        tok_line.err_msg =
                            Some(format!("invalid CIDR mask in address \"{full}\""));
                        return Ok(None);
                    }
                }
            } else if parsedline.hostname.is_none() {
                // Read the mask field.
                field += 1;
                if field >= tok_line.fields.len() {
                    report_config(
                        elevel,
                        "parse_hba_line",
                        "end-of-line before netmask specification".to_string(),
                        Some(
                            "Specify an address range in CIDR notation, or provide a separate netmask.",
                        ),
                        line_num,
                        &file_name,
                    )?;
                    tok_line.err_msg =
                        Some("end-of-line before netmask specification".to_string());
                    return Ok(None);
                }
                let tokens = tok_line.fields[field].clone();
                if tokens.len() > 1 {
                    report_config(
                        elevel,
                        "parse_hba_line",
                        "multiple values specified for netmask".to_string(),
                        None,
                        line_num,
                        &file_name,
                    )?;
                    tok_line.err_msg = Some("multiple values specified for netmask".to_string());
                    return Ok(None);
                }
                let mtoken = &tokens[0];
                let mstr = String::from_utf8_lossy(tok_str(mtoken)).into_owned();

                let hint = numeric_host_hint();
                let mut gai_result: Vec<::net::PgAddrInfo> = Vec::new();
                let ret = ip::pg_getaddrinfo_all(Some(&mstr), None, &hint, &mut gai_result);
                if ret != 0 || gai_result.is_empty() {
                    report_config(
                        elevel,
                        "parse_hba_line",
                        format!("invalid IP mask \"{mstr}\": {}", gai_strerror(ret)),
                        None,
                        line_num,
                        &file_name,
                    )?;
                    tok_line.err_msg =
                        Some(format!("invalid IP mask \"{mstr}\": {}", gai_strerror(ret)));
                    return Ok(None);
                }

                // memcpy(&parsedline->mask, gai_result->ai_addr, ai_addrlen);
                parsedline.mask = gai_result[0].addr.addr;
                parsedline.masklen = gai_result[0].addr.salen as i32;

                // if (addr.ss_family != mask.ss_family) ...
                if ss_family(&addr_view(&parsedline)) != ss_family(&mask_view(&parsedline)) {
                    report_config(
                        elevel,
                        "parse_hba_line",
                        "IP address and mask do not match".to_string(),
                        None,
                        line_num,
                        &file_name,
                    )?;
                    tok_line.err_msg = Some("IP address and mask do not match".to_string());
                    return Ok(None);
                }
            }
        }
    } // != ctLocal

    // Get the authentication method.
    field += 1;
    if field >= tok_line.fields.len() {
        report_config(
            elevel,
            "parse_hba_line",
            "end-of-line before authentication method".to_string(),
            None,
            line_num,
            &file_name,
        )?;
        tok_line.err_msg = Some("end-of-line before authentication method".to_string());
        return Ok(None);
    }
    let tokens = tok_line.fields[field].clone();
    if tokens.len() > 1 {
        report_config(
            elevel,
            "parse_hba_line",
            "multiple values specified for authentication type".to_string(),
            Some("Specify exactly one authentication type per line."),
            line_num,
            &file_name,
        )?;
        tok_line.err_msg = Some("multiple values specified for authentication type".to_string());
        return Ok(None);
    }
    let token = &tokens[0];
    let ts = tok_str(token).to_vec();

    let mut unsupauth: Option<&str> = None;
    if ts == b"trust" {
        parsedline.auth_method = uaTrust;
    } else if ts == b"ident" {
        parsedline.auth_method = uaIdent;
    } else if ts == b"peer" {
        parsedline.auth_method = uaPeer;
    } else if ts == b"password" {
        parsedline.auth_method = uaPassword;
    } else if ts == b"gss" {
        if enable_gss() {
            parsedline.auth_method = uaGSS;
        } else {
            unsupauth = Some("gss");
        }
    } else if ts == b"sspi" {
        if enable_sspi() {
            parsedline.auth_method = uaSSPI;
        } else {
            unsupauth = Some("sspi");
        }
    } else if ts == b"reject" {
        parsedline.auth_method = uaReject;
    } else if ts == b"md5" {
        parsedline.auth_method = uaMD5;
    } else if ts == b"scram-sha-256" {
        parsedline.auth_method = uaSCRAM;
    } else if ts == b"pam" {
        if use_pam() {
            parsedline.auth_method = uaPAM;
        } else {
            unsupauth = Some("pam");
        }
    } else if ts == b"bsd" {
        if use_bsd_auth() {
            parsedline.auth_method = uaBSD;
        } else {
            unsupauth = Some("bsd");
        }
    } else if ts == b"ldap" {
        if use_ldap() {
            parsedline.auth_method = uaLDAP;
        } else {
            unsupauth = Some("ldap");
        }
    } else if ts == b"cert" {
        if use_ssl() {
            parsedline.auth_method = uaCert;
        } else {
            unsupauth = Some("cert");
        }
    } else if ts == b"radius" {
        parsedline.auth_method = uaRADIUS;
    } else if ts == b"oauth" {
        parsedline.auth_method = uaOAuth;
    } else {
        let s = String::from_utf8_lossy(&ts);
        report_config(
            elevel,
            "parse_hba_line",
            format!("invalid authentication method \"{s}\""),
            None,
            line_num,
            &file_name,
        )?;
        tok_line.err_msg = Some(format!("invalid authentication method \"{s}\""));
        return Ok(None);
    }

    if let Some(_ua) = unsupauth {
        let s = String::from_utf8_lossy(&ts);
        report_config(
            elevel,
            "parse_hba_line",
            format!("invalid authentication method \"{s}\": not supported by this build"),
            None,
            line_num,
            &file_name,
        )?;
        tok_line.err_msg = Some(format!(
            "invalid authentication method \"{s}\": not supported by this build"
        ));
        return Ok(None);
    }

    // XXX: When using ident on local connections, change it to peer.
    if parsedline.conntype == ctLocal && parsedline.auth_method == uaIdent {
        parsedline.auth_method = uaPeer;
    }

    // Invalid authentication combinations.
    if parsedline.conntype == ctLocal && parsedline.auth_method == uaGSS {
        report_config(
            elevel,
            "parse_hba_line",
            "gssapi authentication is not supported on local sockets".to_string(),
            None,
            line_num,
            &file_name,
        )?;
        tok_line.err_msg =
            Some("gssapi authentication is not supported on local sockets".to_string());
        return Ok(None);
    }

    if parsedline.conntype != ctLocal && parsedline.auth_method == uaPeer {
        report_config(
            elevel,
            "parse_hba_line",
            "peer authentication is only supported on local sockets".to_string(),
            None,
            line_num,
            &file_name,
        )?;
        tok_line.err_msg =
            Some("peer authentication is only supported on local sockets".to_string());
        return Ok(None);
    }

    if parsedline.conntype != ctHostSSL && parsedline.auth_method == uaCert {
        report_config(
            elevel,
            "parse_hba_line",
            "cert authentication is only supported on hostssl connections".to_string(),
            None,
            line_num,
            &file_name,
        )?;
        tok_line.err_msg =
            Some("cert authentication is only supported on hostssl connections".to_string());
        return Ok(None);
    }

    // For GSS and SSPI, default include_realm to true.
    if parsedline.auth_method == uaGSS || parsedline.auth_method == uaSSPI {
        parsedline.include_realm = true;
    }

    // For SSPI, compat_realm defaults to true and upn_username false.
    if parsedline.auth_method == uaSSPI {
        parsedline.compat_realm = true;
        parsedline.upn_username = false;
    }

    // Parse remaining arguments.
    field += 1;
    while field < tok_line.fields.len() {
        let tokens = tok_line.fields[field].clone();
        for token in &tokens {
            let raw = String::from_utf8_lossy(tok_str(token)).into_owned();
            match raw.find('=') {
                None => {
                    let full = String::from_utf8_lossy(tok_str(token));
                    report_config(
                        elevel,
                        "parse_hba_line",
                        format!("authentication option not in name=value format: {full}"),
                        None,
                        line_num,
                        &file_name,
                    )?;
                    tok_line.err_msg = Some(format!(
                        "authentication option not in name=value format: {full}"
                    ));
                    return Ok(None);
                }
                Some(pos) => {
                    let name = raw[..pos].to_string();
                    let val = raw[pos + 1..].to_string();
                    if !parse_hba_auth_opt(
                        &name,
                        &val,
                        &mut parsedline,
                        elevel,
                        &mut tok_line.err_msg,
                    )? {
                        return Ok(None);
                    }
                }
            }
        }
        field += 1;
    }

    // Check mandatory arguments per method.
    if parsedline.auth_method == uaLDAP {
        if !have_ldap_initialize() && parsedline.ldapserver.is_none() {
            // MANDATORY_AUTH_ARG(ldapserver, "ldapserver", "ldap")
            report_config(
                elevel,
                "parse_hba_line",
                "authentication method \"ldap\" requires argument \"ldapserver\" to be set"
                    .to_string(),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some(
                "authentication method \"ldap\" requires argument \"ldapserver\" to be set"
                    .to_string(),
            );
            return Ok(None);
        }

        if parsedline.ldapprefix.is_some() || parsedline.ldapsuffix.is_some() {
            if parsedline.ldapbasedn.is_some()
                || parsedline.ldapbinddn.is_some()
                || parsedline.ldapbindpasswd.is_some()
                || parsedline.ldapsearchattribute.is_some()
                || parsedline.ldapsearchfilter.is_some()
            {
                report_config(
                    elevel,
                    "parse_hba_line",
                    "cannot mix options for simple bind and search+bind modes".to_string(),
                    None,
                    line_num,
                    &file_name,
                )?;
                tok_line.err_msg =
                    Some("cannot mix options for simple bind and search+bind modes".to_string());
                return Ok(None);
            }
        } else if parsedline.ldapbasedn.is_none() {
            report_config(
                elevel,
                "parse_hba_line",
                "authentication method \"ldap\" requires argument \"ldapbasedn\", \"ldapprefix\", or \"ldapsuffix\" to be set".to_string(),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some("authentication method \"ldap\" requires argument \"ldapbasedn\", \"ldapprefix\", or \"ldapsuffix\" to be set".to_string());
            return Ok(None);
        }

        if parsedline.ldapsearchattribute.is_some() && parsedline.ldapsearchfilter.is_some() {
            report_config(
                elevel,
                "parse_hba_line",
                "cannot use ldapsearchattribute together with ldapsearchfilter".to_string(),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg =
                Some("cannot use ldapsearchattribute together with ldapsearchfilter".to_string());
            return Ok(None);
        }
    }

    if parsedline.auth_method == uaRADIUS {
        if parsedline.radiusservers.is_empty() {
            // MANDATORY_AUTH_ARG(radiusservers, ...) — radiusservers list NIL.
            report_config(
                elevel,
                "parse_hba_line",
                "authentication method \"radius\" requires argument \"radiusservers\" to be set"
                    .to_string(),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some(
                "authentication method \"radius\" requires argument \"radiusservers\" to be set"
                    .to_string(),
            );
            return Ok(None);
        }
        if parsedline.radiussecrets.is_empty() {
            report_config(
                elevel,
                "parse_hba_line",
                "authentication method \"radius\" requires argument \"radiussecrets\" to be set"
                    .to_string(),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some(
                "authentication method \"radius\" requires argument \"radiussecrets\" to be set"
                    .to_string(),
            );
            return Ok(None);
        }

        // list_length(secrets) must be 1 or == #servers.
        let nserv = parsedline.radiusservers.len();
        if !(parsedline.radiussecrets.len() == 1 || parsedline.radiussecrets.len() == nserv) {
            report_config(
                elevel,
                "parse_hba_line",
                format!(
                    "the number of RADIUS secrets ({}) must be 1 or the same as the number of RADIUS servers ({})",
                    parsedline.radiussecrets.len(),
                    nserv
                ),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some(format!(
                "the number of RADIUS secrets ({}) must be 1 or the same as the number of RADIUS servers ({})",
                parsedline.radiussecrets.len(),
                nserv
            ));
            return Ok(None);
        }
        if !(parsedline.radiusports.is_empty()
            || parsedline.radiusports.len() == 1
            || parsedline.radiusports.len() == nserv)
        {
            report_config(
                elevel,
                "parse_hba_line",
                format!(
                    "the number of RADIUS ports ({}) must be 1 or the same as the number of RADIUS servers ({})",
                    parsedline.radiusports.len(),
                    nserv
                ),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some(format!(
                "the number of RADIUS ports ({}) must be 1 or the same as the number of RADIUS servers ({})",
                parsedline.radiusports.len(),
                nserv
            ));
            return Ok(None);
        }
        if !(parsedline.radiusidentifiers.is_empty()
            || parsedline.radiusidentifiers.len() == 1
            || parsedline.radiusidentifiers.len() == nserv)
        {
            report_config(
                elevel,
                "parse_hba_line",
                format!(
                    "the number of RADIUS identifiers ({}) must be 1 or the same as the number of RADIUS servers ({})",
                    parsedline.radiusidentifiers.len(),
                    nserv
                ),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some(format!(
                "the number of RADIUS identifiers ({}) must be 1 or the same as the number of RADIUS servers ({})",
                parsedline.radiusidentifiers.len(),
                nserv
            ));
            return Ok(None);
        }
    }

    // Enforce any parameters implied by other settings.
    if parsedline.auth_method == uaCert {
        // For cert, client certificate validation is mandatory (verify-full).
        parsedline.clientcert = clientCertFull;
    }

    // Enforce proper configuration of OAuth authentication.
    if parsedline.auth_method == uaOAuth {
        if parsedline.oauth_scope.is_none() {
            report_config(
                elevel,
                "parse_hba_line",
                "authentication method \"oauth\" requires argument \"scope\" to be set".to_string(),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some(
                "authentication method \"oauth\" requires argument \"scope\" to be set".to_string(),
            );
            return Ok(None);
        }
        if parsedline.oauth_issuer.is_none() {
            report_config(
                elevel,
                "parse_hba_line",
                "authentication method \"oauth\" requires argument \"issuer\" to be set"
                    .to_string(),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg = Some(
                "authentication method \"oauth\" requires argument \"issuer\" to be set"
                    .to_string(),
            );
            return Ok(None);
        }

        // Ensure a validator library is set and permitted by the config.
        // `auth-oauth.c` owns the validator-library machinery (unported); the
        // call crosses its seam, which panics until that owner lands.
        let (ok, oauth_err) = oauth_seams::check_oauth_validator::call(
            parsedline.clone(),
            elevel.0,
        )?;
        if oauth_err.is_some() {
            tok_line.err_msg = oauth_err;
        }
        if !ok {
            return Ok(None);
        }

        // A usermap with delegate_ident_mapping is nonsensical.
        if parsedline.oauth_skip_usermap && parsedline.usermap.is_some() {
            report_config(
                elevel,
                "parse_hba_line",
                format!("{} cannot be used in combination with {}", "map", "delegate_ident_mapping"),
                None,
                line_num,
                &file_name,
            )?;
            tok_line.err_msg =
                Some("map cannot be used in combination with delegate_ident_mapping".to_string());
            return Ok(None);
        }
    }

    Ok(Some(parsedline))
}

// --- views over the addr/mask byte buffers (for ss_family) -----------------

fn addr_view(p: &HbaLine) -> ::net::SockAddr {
    ::net::SockAddr { addr: p.addr, salen: p.addrlen as u32 }
}
fn mask_view(p: &HbaLine) -> ::net::SockAddr {
    ::net::SockAddr { addr: p.mask, salen: p.masklen as u32 }
}
/// `parsedline->addr.ss_family` even before addrlen is set (for cidr mask): the
/// family is encoded in the stored bytes; use addrlen if set, else probe the
/// raw family.
fn sockaddr_view(p: &HbaLine) -> ::net::SockAddr {
    ::net::SockAddr {
        addr: p.addr,
        salen: if p.addrlen > 0 {
            p.addrlen as u32
        } else {
            core::mem::size_of::<libc::sockaddr_storage>() as u32
        },
    }
}

/// `static bool parse_hba_auth_opt(char *name, char *val, HbaLine *hbaline, int
/// elevel, char **err_msg)` (hba.c:2086).
pub(crate) fn parse_hba_auth_opt(
    name: &str,
    val: &str,
    hbaline: &mut HbaLine,
    elevel: ErrorLevel,
    err_msg: &mut Option<String>,
) -> PgResult<bool> {
    let line_num = hbaline.linenumber;
    let file_name = hbaline.sourcefile.clone().unwrap_or_default();

    // INVALID_AUTH_OPTION(optname, validmethods): report + set err_msg + return.
    macro_rules! invalid_auth_option {
        ($optname:expr, $validmethods:expr) => {{
            report_config(
                elevel,
                "parse_hba_auth_opt",
                format!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    $optname, $validmethods
                ),
                None,
                line_num,
                &file_name,
            )?;
            *err_msg = Some(format!(
                "authentication option \"{}\" is only valid for authentication methods {}",
                $optname, $validmethods
            ));
            return Ok(false);
        }};
    }

    // REQUIRE_AUTH_OPTION(methodval, optname, validmethods).
    macro_rules! require_auth_option {
        ($methodval:expr, $optname:expr, $validmethods:expr) => {{
            if hbaline.auth_method != $methodval {
                invalid_auth_option!($optname, $validmethods);
            }
        }};
    }

    if name == "map" {
        if hbaline.auth_method != uaIdent
            && hbaline.auth_method != uaPeer
            && hbaline.auth_method != uaGSS
            && hbaline.auth_method != uaSSPI
            && hbaline.auth_method != uaCert
            && hbaline.auth_method != uaOAuth
        {
            invalid_auth_option!("map", "ident, peer, gssapi, sspi, cert, and oauth");
        }
        hbaline.usermap = Some(val.to_string());
    } else if name == "clientcert" {
        if hbaline.conntype != ctHostSSL {
            report_config(
                elevel,
                "parse_hba_auth_opt",
                "clientcert can only be configured for \"hostssl\" rows".to_string(),
                None,
                line_num,
                &file_name,
            )?;
            *err_msg = Some("clientcert can only be configured for \"hostssl\" rows".to_string());
            return Ok(false);
        }
        if val == "verify-full" {
            hbaline.clientcert = clientCertFull;
        } else if val == "verify-ca" {
            if hbaline.auth_method == uaCert {
                report_config(
                    elevel,
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
                "parse_hba_auth_opt",
                format!("invalid value for clientcert: \"{val}\""),
                None,
                line_num,
                &file_name,
            )?;
            return Ok(false);
        }
    } else if name == "clientname" {
        if hbaline.conntype != ctHostSSL {
            report_config(
                elevel,
                "parse_hba_auth_opt",
                "clientname can only be configured for \"hostssl\" rows".to_string(),
                None,
                line_num,
                &file_name,
            )?;
            *err_msg = Some("clientname can only be configured for \"hostssl\" rows".to_string());
            return Ok(false);
        }
        if val == "CN" {
            hbaline.clientcertname = clientCertCN;
        } else if val == "DN" {
            hbaline.clientcertname = clientCertDN;
        } else {
            report_config(
                elevel,
                "parse_hba_auth_opt",
                format!("invalid value for clientname: \"{val}\""),
                None,
                line_num,
                &file_name,
            )?;
            return Ok(false);
        }
    } else if name == "pamservice" {
        require_auth_option!(uaPAM, "pamservice", "pam");
        hbaline.pamservice = Some(val.to_string());
    } else if name == "pam_use_hostname" {
        require_auth_option!(uaPAM, "pam_use_hostname", "pam");
        hbaline.pam_use_hostname = val == "1";
    } else if name == "ldapurl" {
        require_auth_option!(uaLDAP, "ldapurl", "ldap");
        if ldap_api_feature_x_openldap() {
            // OpenLDAP `ldap_url_parse` arm — dead in this build (the predicate
            // is false), so the not-supported branch below is taken.
            unreachable!("ldap_api_feature_x_openldap is false in this build");
        } else {
            // not OpenLDAP
            report_config_feature(
                elevel,
                "LDAP URLs not supported on this platform".to_string(),
            )?;
            *err_msg = Some("LDAP URLs not supported on this platform".to_string());
        }
    } else if name == "ldaptls" {
        require_auth_option!(uaLDAP, "ldaptls", "ldap");
        hbaline.ldaptls = val == "1";
    } else if name == "ldapscheme" {
        require_auth_option!(uaLDAP, "ldapscheme", "ldap");
        if val != "ldap" && val != "ldaps" {
            report_config(
                elevel,
                "parse_hba_auth_opt",
                format!("invalid ldapscheme value: \"{val}\""),
                None,
                line_num,
                &file_name,
            )?;
        }
        hbaline.ldapscheme = Some(val.to_string());
    } else if name == "ldapserver" {
        require_auth_option!(uaLDAP, "ldapserver", "ldap");
        hbaline.ldapserver = Some(val.to_string());
    } else if name == "ldapport" {
        require_auth_option!(uaLDAP, "ldapport", "ldap");
        hbaline.ldapport = atoi(val);
        if hbaline.ldapport == 0 {
            report_config(
                elevel,
                "parse_hba_auth_opt",
                format!("invalid LDAP port number: \"{val}\""),
                None,
                line_num,
                &file_name,
            )?;
            *err_msg = Some(format!("invalid LDAP port number: \"{val}\""));
            return Ok(false);
        }
    } else if name == "ldapbinddn" {
        require_auth_option!(uaLDAP, "ldapbinddn", "ldap");
        hbaline.ldapbinddn = Some(val.to_string());
    } else if name == "ldapbindpasswd" {
        require_auth_option!(uaLDAP, "ldapbindpasswd", "ldap");
        hbaline.ldapbindpasswd = Some(val.to_string());
    } else if name == "ldapsearchattribute" {
        require_auth_option!(uaLDAP, "ldapsearchattribute", "ldap");
        hbaline.ldapsearchattribute = Some(val.to_string());
    } else if name == "ldapsearchfilter" {
        require_auth_option!(uaLDAP, "ldapsearchfilter", "ldap");
        hbaline.ldapsearchfilter = Some(val.to_string());
    } else if name == "ldapbasedn" {
        require_auth_option!(uaLDAP, "ldapbasedn", "ldap");
        hbaline.ldapbasedn = Some(val.to_string());
    } else if name == "ldapprefix" {
        require_auth_option!(uaLDAP, "ldapprefix", "ldap");
        hbaline.ldapprefix = Some(val.to_string());
    } else if name == "ldapsuffix" {
        require_auth_option!(uaLDAP, "ldapsuffix", "ldap");
        hbaline.ldapsuffix = Some(val.to_string());
    } else if name == "krb_realm" {
        if hbaline.auth_method != uaGSS && hbaline.auth_method != uaSSPI {
            invalid_auth_option!("krb_realm", "gssapi and sspi");
        }
        hbaline.krb_realm = Some(val.to_string());
    } else if name == "include_realm" {
        if hbaline.auth_method != uaGSS && hbaline.auth_method != uaSSPI {
            invalid_auth_option!("include_realm", "gssapi and sspi");
        }
        hbaline.include_realm = val == "1";
    } else if name == "compat_realm" {
        if hbaline.auth_method != uaSSPI {
            invalid_auth_option!("compat_realm", "sspi");
        }
        hbaline.compat_realm = val == "1";
    } else if name == "upn_username" {
        if hbaline.auth_method != uaSSPI {
            invalid_auth_option!("upn_username", "sspi");
        }
        hbaline.upn_username = val == "1";
    } else if name == "radiusservers" {
        require_auth_option!(uaRADIUS, "radiusservers", "radius");
        let parsed = match split_guc_list(val) {
            Some(l) => l,
            None => {
                report_config(
                    elevel,
                    "parse_hba_auth_opt",
                    format!("could not parse RADIUS server list \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            }
        };
        // For each entry in the list, translate it.
        for srv in &parsed {
            let hint = ::net::AddrInfoHint {
                flags: 0,
                family: libc::AF_UNSPEC,
                socktype: libc::SOCK_DGRAM,
            };
            let mut gai_result: Vec<::net::PgAddrInfo> = Vec::new();
            let ret = ip::pg_getaddrinfo_all(Some(srv), None, &hint, &mut gai_result);
            if ret != 0 || gai_result.is_empty() {
                report_config(
                    elevel,
                    "parse_hba_auth_opt",
                    format!(
                        "could not translate RADIUS server name \"{srv}\" to address: {}",
                        gai_strerror(ret)
                    ),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            }
        }
        hbaline.radiusservers = parsed;
        hbaline.radiusservers_s = Some(val.to_string());
    } else if name == "radiusports" {
        require_auth_option!(uaRADIUS, "radiusports", "radius");
        let parsed = match split_guc_list(val) {
            Some(l) => l,
            None => {
                report_config(
                    elevel,
                    "parse_hba_auth_opt",
                    format!("could not parse RADIUS port list \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                *err_msg = Some(format!("invalid RADIUS port number: \"{val}\""));
                return Ok(false);
            }
        };
        for p in &parsed {
            if atoi(p) == 0 {
                report_config(
                    elevel,
                    "parse_hba_auth_opt",
                    format!("invalid RADIUS port number: \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            }
        }
        hbaline.radiusports = parsed;
        hbaline.radiusports_s = Some(val.to_string());
    } else if name == "radiussecrets" {
        require_auth_option!(uaRADIUS, "radiussecrets", "radius");
        let parsed = match split_guc_list(val) {
            Some(l) => l,
            None => {
                report_config(
                    elevel,
                    "parse_hba_auth_opt",
                    format!("could not parse RADIUS secret list \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            }
        };
        hbaline.radiussecrets = parsed;
        hbaline.radiussecrets_s = Some(val.to_string());
    } else if name == "radiusidentifiers" {
        require_auth_option!(uaRADIUS, "radiusidentifiers", "radius");
        let parsed = match split_guc_list(val) {
            Some(l) => l,
            None => {
                report_config(
                    elevel,
                    "parse_hba_auth_opt",
                    format!("could not parse RADIUS identifiers list \"{val}\""),
                    None,
                    line_num,
                    &file_name,
                )?;
                return Ok(false);
            }
        };
        hbaline.radiusidentifiers = parsed;
        hbaline.radiusidentifiers_s = Some(val.to_string());
    } else if name == "issuer" {
        require_auth_option!(uaOAuth, "issuer", "oauth");
        hbaline.oauth_issuer = Some(val.to_string());
    } else if name == "scope" {
        require_auth_option!(uaOAuth, "scope", "oauth");
        hbaline.oauth_scope = Some(val.to_string());
    } else if name == "validator" {
        require_auth_option!(uaOAuth, "validator", "oauth");
        hbaline.oauth_validator = Some(val.to_string());
    } else if name == "delegate_ident_mapping" {
        require_auth_option!(uaOAuth, "delegate_ident_mapping", "oauth");
        hbaline.oauth_skip_usermap = val == "1";
    } else {
        report_config(
            elevel,
            "parse_hba_auth_opt",
            format!("unrecognized authentication option name: \"{name}\""),
            None,
            line_num,
            &file_name,
        )?;
        *err_msg = Some(format!("unrecognized authentication option name: \"{name}\""));
        return Ok(false);
    }
    Ok(true)
}

/// `ereport(elevel, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(...)))` —
/// the `ldapurl` not-supported report (no errcontext, matching C).
fn report_config_feature(elevel: ErrorLevel, msg: String) -> PgResult<()> {
    crate::ereport(elevel)
        .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(msg)
        .finish(crate::here("parse_hba_auth_opt"))
}

/// `atoi(s)` — C parse-leading-integer semantics (stops at first non-digit;
/// `0` on no digits).
fn atoi(s: &str) -> i32 {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut sign = 1i64;
    while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
        i += 1;
    }
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        if bytes[i] == b'-' {
            sign = -1;
        }
        i += 1;
    }
    let mut acc: i64 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        acc = acc.saturating_mul(10).saturating_add((bytes[i] - b'0') as i64);
        i += 1;
    }
    (sign * acc).clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

/// `SplitGUCList(dupval, ',', &parsed)` (varlena.c). Returns the comma-split
/// items as owned `String`s, or `None` on a list syntax error (`false` return
/// in C).
fn split_guc_list(rawstring: &str) -> Option<Vec<String>> {
    let memctx = MemCtx::new("split_guc_list");
    let mcx = memctx.mcx();
    let result = varlena::split_format::split_guc_list(mcx, rawstring, ',');
    match result {
        Ok(Some(list)) => {
            let out: Vec<String> = list.iter().map(|s| s.as_str().to_string()).collect();
            Some(out)
        }
        Ok(None) => None,
        // SplitGUCList only fails by returning false (None); a builder OOM would
        // propagate, but the list parse itself does not ereport.
        Err(_) => None,
    }
}

// Keep the line_context import referenced (used via report_config; documents the
// errcontext rendering origin).
const _: fn(i32, &str) -> String = line_context;
