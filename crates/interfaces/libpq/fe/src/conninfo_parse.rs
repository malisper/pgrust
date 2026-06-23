//! `PQconninfoParse` — the keyword=value form of the libpq conninfo parser
//! (`fe-connect.c`'s `parse_connection_string` → `conninfo_parse` →
//! `conninfo_storeval`/`conninfo_find`).
//!
//! This is a faithful port of the keyword/value tokenizer and the option-table
//! keyword validation. The URI form (`postgresql://…`) dispatch in
//! `parse_connection_string` (`conninfo_uri_parse`) is not yet ported; a URI
//! prefix is reported as the C "missing \"=\"" tokenizer error would not apply,
//! so we surface a clear unsupported-URI error rather than silently accepting.
//!
//! The result is the list of `(keyword, value)` options the user explicitly
//! supplied (matching `PQconninfoParse`, which returns only the parsed values,
//! not defaults — `use_defaults = false`).

use ::types_libpqwalreceiver::ConninfoOption;

/// The recognized libpq connection keywords (`PQconninfoOptions[].keyword`,
/// `fe-connect.c`). `conninfo_find` accepts exactly these; anything else is the
/// C `invalid connection option "%s"` error.
const PQ_CONNINFO_KEYWORDS: &[&str] = &[
    "service",
    "user",
    "password",
    "passfile",
    "channel_binding",
    "connect_timeout",
    "dbname",
    "host",
    "hostaddr",
    "port",
    "client_encoding",
    "options",
    "application_name",
    "fallback_application_name",
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_count",
    "tcp_user_timeout",
    "sslmode",
    "sslnegotiation",
    "sslcompression",
    "sslcert",
    "sslkey",
    "sslcertmode",
    "sslpassword",
    "sslrootcert",
    "sslcrl",
    "sslcrldir",
    "sslsni",
    "requirepeer",
    "require_auth",
    "min_protocol_version",
    "max_protocol_version",
    "ssl_min_protocol_version",
    "ssl_max_protocol_version",
    "gssencmode",
    "krbsrvname",
    "gsslib",
    "gssdelegation",
    "replication",
    "target_session_attrs",
    "load_balance_hosts",
    "scram_client_key",
    "scram_server_key",
    "oauth_issuer",
    "oauth_client_id",
    "oauth_client_secret",
    "oauth_scope",
    "sslkeylogfile",
];

/// `uri_prefix_length(connstr)` (fe-connect.c) — non-zero if the string starts
/// with `postgresql://` or `postgres://`.
pub(crate) fn uri_prefix_length(s: &str) -> usize {
    const URIS: [&str; 2] = ["postgresql://", "postgres://"];
    for p in URIS {
        if s.starts_with(p) {
            return p.len();
        }
    }
    0
}

/// `conninfo_find(connOptions, keyword)` — whether `keyword` is a recognized
/// connection option.
fn conninfo_find(keyword: &str) -> bool {
    PQ_CONNINFO_KEYWORDS.contains(&keyword)
}

/// `PQconninfoParse(conninfo, &errmsg)` (fe-connect.c). `Ok(options)` is the C
/// non-NULL return; `Err(Some(msg))` is the C NULL return with the error
/// string. Only the explicitly-supplied options are returned (`use_defaults =
/// false`).
pub fn pq_conninfo_parse(conninfo: &str) -> Result<Vec<ConninfoOption>, Option<String>> {
    if uri_prefix_length(conninfo) != 0 {
        // parse_connection_string would dispatch to conninfo_uri_parse, which
        // is not yet ported. Surface a clear error rather than mis-parsing.
        return Err(Some(
            "URI connection strings are not yet supported by this build".to_string(),
        ));
    }
    conninfo_parse(conninfo)
}

/// `conninfo_parse(conninfo, errorMessage, use_defaults=false)` (fe-connect.c).
fn conninfo_parse(conninfo: &str) -> Result<Vec<ConninfoOption>, Option<String>> {
    let bytes: Vec<char> = conninfo.chars().collect();
    let n = bytes.len();
    let mut i = 0usize;
    let mut options: Vec<ConninfoOption> = Vec::new();

    while i < n {
        // Skip blanks before the parameter name.
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Get the parameter name (up to '=' or whitespace).
        let name_start = i;
        let mut name_end = i;
        let mut have_eq = false;
        while i < n {
            if bytes[i] == '=' {
                name_end = i;
                have_eq = true;
                break;
            }
            if bytes[i].is_ascii_whitespace() {
                name_end = i;
                i += 1;
                // Skip whitespace, then expect '='.
                while i < n && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                have_eq = i < n && bytes[i] == '=';
                break;
            }
            i += 1;
            name_end = i;
        }

        let pname: String = bytes[name_start..name_end].iter().collect();

        // Check that there is a following '='.
        if !have_eq {
            // libpq_append_error appends a trailing newline to the buffer.
            return Err(Some(format!(
                "missing \"=\" after \"{pname}\" in connection info string\n"
            )));
        }
        // Consume the '='.
        i += 1;

        // Skip blanks after the '='.
        while i < n && bytes[i].is_ascii_whitespace() {
            i += 1;
        }

        // Get the parameter value.
        let mut pval = String::new();
        if i < n && bytes[i] == '\'' {
            // Single-quoted value.
            i += 1;
            loop {
                if i >= n {
                    return Err(Some(
                        "unterminated quoted string in connection info string\n".to_string(),
                    ));
                }
                if bytes[i] == '\\' {
                    i += 1;
                    if i < n {
                        pval.push(bytes[i]);
                        i += 1;
                    }
                    continue;
                }
                if bytes[i] == '\'' {
                    i += 1;
                    break;
                }
                pval.push(bytes[i]);
                i += 1;
            }
        } else {
            // Unquoted value (up to whitespace), honoring backslash escapes.
            while i < n {
                if bytes[i].is_ascii_whitespace() {
                    i += 1;
                    break;
                }
                if bytes[i] == '\\' {
                    i += 1;
                    if i < n {
                        pval.push(bytes[i]);
                        i += 1;
                    }
                    continue;
                }
                pval.push(bytes[i]);
                i += 1;
            }
        }

        // conninfo_storeval: validate the keyword.
        if !conninfo_find(&pname) {
            return Err(Some(format!("invalid connection option \"{pname}\"\n")));
        }

        // Store (the last value for a duplicated keyword wins, as in C).
        if let Some(existing) = options.iter_mut().find(|o| o.keyword == pname) {
            existing.val = Some(pval);
        } else {
            options.push(ConninfoOption {
                keyword: pname,
                val: Some(pval),
                dispchar: String::new(),
            });
        }
    }

    Ok(options)
}
