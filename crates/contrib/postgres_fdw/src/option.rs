use mcx::{Mcx, PgVec};
use types_core::{
    Oid, ATTRIBUTE_RELATION_ID, FOREIGN_SERVER_RELATION_ID, FOREIGN_TABLE_RELATION_ID,
    USER_MAPPING_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_FDW_INVALID_OPTION_NAME, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_OBJECT, WARNING,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_nodes::parsenodes::{DefElem, DefElemAction};
use types_nodes::Node;

const SERVER: Oid = FOREIGN_SERVER_RELATION_ID;
const TABLE: Oid = FOREIGN_TABLE_RELATION_ID;
const MAPPING: Oid = USER_MAPPING_RELATION_ID;
const ATTR: Oid = ATTRIBUTE_RELATION_ID;

// InitPgFdwOptions (option.c). Divergence: C appends PQconndefaults() output
// at backend init; without libpq linked, the libpq half is transcribed from
// libpq 18.3's PQconninfoOptions (fe-connect.c) with C's filters pre-applied
// (dispchar 'D', fallback_application_name, client_encoding, oauth_* dropped;
// "user" and dispchar-'*' entries land on user mappings). Same keywords, same
// order, so validator error texts and hints match byte-for-byte.
pub(crate) const POSTGRES_FDW_OPTIONS: &[(&str, Oid, bool)] = &[
    ("service", SERVER, true),
    ("user", MAPPING, true),
    ("password", MAPPING, true),
    ("passfile", SERVER, true),
    ("channel_binding", SERVER, true),
    ("connect_timeout", SERVER, true),
    ("dbname", SERVER, true),
    ("host", SERVER, true),
    ("hostaddr", SERVER, true),
    ("port", SERVER, true),
    ("options", SERVER, true),
    ("application_name", SERVER, true),
    ("keepalives", SERVER, true),
    ("keepalives_idle", SERVER, true),
    ("keepalives_interval", SERVER, true),
    ("keepalives_count", SERVER, true),
    ("tcp_user_timeout", SERVER, true),
    ("sslmode", SERVER, true),
    ("sslnegotiation", SERVER, true),
    ("sslcompression", SERVER, true),
    ("sslcert", SERVER, true),
    ("sslkey", SERVER, true),
    ("sslcertmode", SERVER, true),
    ("sslpassword", MAPPING, true),
    ("sslrootcert", SERVER, true),
    ("sslcrl", SERVER, true),
    ("sslcrldir", SERVER, true),
    ("sslsni", SERVER, true),
    ("requirepeer", SERVER, true),
    ("require_auth", SERVER, true),
    ("min_protocol_version", SERVER, true),
    ("max_protocol_version", SERVER, true),
    ("ssl_min_protocol_version", SERVER, true),
    ("ssl_max_protocol_version", SERVER, true),
    ("gssencmode", SERVER, true),
    ("krbsrvname", SERVER, true),
    ("gsslib", SERVER, true),
    ("gssdelegation", SERVER, true),
    ("target_session_attrs", SERVER, true),
    ("load_balance_hosts", SERVER, true),
    ("schema_name", TABLE, false),
    ("table_name", TABLE, false),
    ("column_name", ATTR, false),
    ("use_remote_estimate", SERVER, false),
    ("use_remote_estimate", TABLE, false),
    ("fdw_startup_cost", SERVER, false),
    ("fdw_tuple_cost", SERVER, false),
    ("extensions", SERVER, false),
    ("updatable", SERVER, false),
    ("updatable", TABLE, false),
    ("truncatable", SERVER, false),
    ("truncatable", TABLE, false),
    ("fetch_size", SERVER, false),
    ("fetch_size", TABLE, false),
    ("batch_size", SERVER, false),
    ("batch_size", TABLE, false),
    ("async_capable", SERVER, false),
    ("async_capable", TABLE, false),
    ("parallel_commit", SERVER, false),
    ("parallel_abort", SERVER, false),
    ("keep_connections", SERVER, false),
    ("password_required", MAPPING, false),
    ("analyze_sampling", SERVER, false),
    ("analyze_sampling", TABLE, false),
    ("use_scram_passthrough", SERVER, false),
    ("use_scram_passthrough", MAPPING, false),
    ("sslcert", MAPPING, true),
    ("sslkey", MAPPING, true),
    ("gssdelegation", MAPPING, true),
];

fn is_valid_option(keyword: &str, context: Oid) -> bool {
    POSTGRES_FDW_OPTIONS.iter().any(|&(k, ctx, _)| ctx == context && k == keyword)
}

pub(crate) fn is_libpq_option(keyword: &str) -> bool {
    POSTGRES_FDW_OPTIONS.iter().any(|&(k, _, is_libpq)| is_libpq && k == keyword)
}

const MAX_LEVENSHTEIN_STRLEN: usize = 255;

#[cold]
fn invalid_option_error(mcx: Mcx<'_>, name: &str, catalog: Oid) -> PgResult<Box<PgError>> {
    let mut min_d = -1i32;
    let mut closest: Option<&str> = None;
    let mut has_valid_options = false;
    for &(k, ctx, _) in POSTGRES_FDW_OPTIONS {
        if ctx != catalog {
            continue;
        }
        has_valid_options = true;
        if name.is_empty() || name.len() > MAX_LEVENSHTEIN_STRLEN || k.len() > MAX_LEVENSHTEIN_STRLEN
        {
            continue;
        }
        let dist = varlena::levenshtein::varstr_levenshtein_less_equal(
            mcx,
            name.as_bytes(),
            k.as_bytes(),
            1,
            1,
            1,
            4,
            true,
        )?;
        if dist <= 4 && dist as usize <= name.len() / 2 && (min_d == -1 || dist < min_d) {
            min_d = dist;
            closest = Some(k);
        }
    }
    let mut e = PgError::error(format!("invalid option \"{name}\""))
        .with_sqlstate(ERRCODE_FDW_INVALID_OPTION_NAME);
    e = if has_valid_options {
        match closest {
            Some(m) => e.with_hint(format!("Perhaps you meant the option \"{m}\".")),
            None => e,
        }
    } else {
        e.with_hint("There are no valid options in this context.")
    };
    Ok(Box::new(e))
}

fn mk_def_elem<'mcx>(
    mcx: Mcx<'mcx>,
    name: &'mcx str,
    value: Option<&'mcx str>,
) -> PgResult<DefElem<'mcx>> {
    // C untransformRelOptions: a text element without '=' yields a DefElem
    // with a NULL arg; defGetBoolean(NULL arg) is true, defGetString errors.
    Ok(DefElem {
        defnamespace: None,
        defname: Some(name),
        arg: value.map(|v| Node::mk(mcx, types_nodes::String { sval: v })).transpose()?,
        defaction: DefElemAction::DEFELEM_UNSPEC,
        location: -1,
    })
}

#[track_caller]
#[cold]
fn invalid_numeric_value(kind: &str, name: &str, value: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid value for {kind} option \"{name}\": {value}"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

pub(crate) fn fc_postgres_fdw_validator(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<datum::Datum> {
    let mcx = fcinfo.result_mcx();
    let options = foreigncmds::options::untransform_options(mcx, Some(fcinfo.arg(0)))?;
    let catalog = fcinfo.arg(1).as_oid();

    for opt in options.iter() {
        if !is_valid_option(opt.name, catalog) {
            return Err(invalid_option_error(mcx, opt.name, catalog)?);
        }

        match opt.name {
            "use_remote_estimate" | "updatable" | "truncatable" | "async_capable"
            | "parallel_commit" | "parallel_abort" | "keep_connections" => {
                commands_define::defGetBoolean(&mk_def_elem(mcx, opt.name, opt.value)?)?;
            }
            "fdw_startup_cost" | "fdw_tuple_cost" => {
                let sval = opt.require_value()?;
                let real_val = match guc::units::parse_real(sval, 0) {
                    guc::units::ParseNum::Ok(v) => v,
                    guc::units::ParseNum::Err { .. } => {
                        return Err(invalid_numeric_value("floating point", opt.name, sval));
                    }
                };
                if real_val < 0.0 {
                    return Err(Box::new(
                        PgError::error(format!(
                            "\"{}\" must be a floating point value greater than or equal to zero",
                            opt.name
                        ))
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                    ));
                }
            }
            "extensions" => {
                extract_extension_list(mcx, opt.require_value()?, true)?;
            }
            "fetch_size" | "batch_size" => {
                let sval = opt.require_value()?;
                let int_val = match guc::units::parse_int(sval, 0) {
                    guc::units::ParseNum::Ok(v) => v,
                    guc::units::ParseNum::Err { .. } => {
                        return Err(invalid_numeric_value("integer", opt.name, sval));
                    }
                };
                if int_val <= 0 {
                    return Err(Box::new(
                        PgError::error(format!(
                            "\"{}\" must be an integer value greater than zero",
                            opt.name
                        ))
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                    ));
                }
            }
            "password_required" => {
                let pw_required =
                    commands_define::defGetBoolean(&mk_def_elem(mcx, opt.name, opt.value)?)?;
                if !superuser::superuser()? && !pw_required {
                    return Err(Box::new(
                        PgError::error("password_required=false is superuser-only")
                            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                            .with_hint(
                                "User mappings with the password_required option set to false \
                                 may only be created or modified by the superuser.",
                            ),
                    ));
                }
            }
            "sslcert" | "sslkey" => {
                if catalog == USER_MAPPING_RELATION_ID && !superuser::superuser()? {
                    return Err(Box::new(
                        PgError::error("sslcert and sslkey are superuser-only")
                            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                            .with_hint(
                                "User mappings with the sslcert or sslkey options set may only \
                                 be created or modified by the superuser.",
                            ),
                    ));
                }
            }
            "analyze_sampling" => {
                let sval = opt.require_value()?;
                if !matches!(sval, "off" | "auto" | "random" | "system" | "bernoulli") {
                    return Err(invalid_numeric_value("string", opt.name, sval));
                }
            }
            _ => {}
        }
    }

    Ok(datum::Datum::null())
}

pub(crate) fn extract_extension_list<'mcx>(
    mcx: Mcx<'mcx>,
    extensions_string: &str,
    warn_on_missing: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    let Some(extlist) = varlena::split_identifier_string(
        mcx,
        extensions_string,
        b',',
        mbutils::GetDatabaseEncoding(),
    )?
    else {
        return Err(Box::new(
            PgError::error("parameter \"extensions\" must be a list of extension names")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    };

    let mut extension_oids: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    for extension_name in &extlist {
        let extension_oid = extension::get_extension_oid(extension_name, true)?;
        if extension_oid != types_core::InvalidOid {
            extension_oids.push(extension_oid);
        } else if warn_on_missing {
            elog::ereport(WARNING)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("extension \"{extension_name}\" is not installed"))
                .finish(crate::loc("ExtractExtensionList"))?;
        }
    }
    Ok(extension_oids)
}

// process_pgfdw_appname (option.c:500): expand the application_name escape
// sequences. C reads MyProcPort's connect-time database/user names; we read
// the live MyDatabaseId / session user (recorded divergence: a renamed
// database or SET SESSION AUTHORIZATION shows the current identity).
pub(crate) fn process_pgfdw_appname(appname: &str) -> PgResult<String> {
    let mut out = String::with_capacity(appname.len());
    let mut chars = appname.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            // Invalid format sequences are ignored, including a trailing '%'.
            None => break,
            Some('%') => out.push('%'),
            Some('a') => {
                if let Ok(Some(v)) = guc::GetConfigOption("application_name", true, false) {
                    out.push_str(&v);
                }
            }
            Some('c') => {
                let start = init_small::globals::MyStartTime();
                let pid = init_small::globals::MyProcPid();
                let _ = core::fmt::Write::write_fmt(
                    &mut out,
                    format_args!("{:x}.{:x}", start, pid),
                );
            }
            Some('C') => {
                if let Ok(Some(v)) = guc::GetConfigOption("cluster_name", true, false) {
                    out.push_str(&v);
                }
            }
            Some('d') => {
                let dbid = init_small::globals::MyDatabaseId();
                match dbcommands_seams::get_database_name::call(dbid) {
                    Ok(Some(name)) => out.push_str(&name),
                    _ => out.push_str("[unknown]"),
                }
            }
            Some('p') => {
                let _ = core::fmt::Write::write_fmt(
                    &mut out,
                    format_args!("{}", init_small::globals::MyProcPid()),
                );
            }
            Some('u') => {
                let scratch = mcx::MemoryContext::new("pgfdw appname");
                let name = {
                    let m = scratch.mcx();
                    miscinit::GetUserNameFromId(m, miscinit::GetSessionUserId(), true)
                        .ok()
                        .flatten()
                        .map(|n| n.as_str().to_string())
                };
                match name {
                    Some(n) => out.push_str(&n),
                    None => out.push_str("[unknown]"),
                }
            }
            // Unrecognized escapes are format errors: ignored, as C.
            Some(_) => {}
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_matrix() {
        assert!(is_valid_option("host", SERVER));
        assert!(!is_valid_option("host", TABLE));
        assert!(is_valid_option("user", MAPPING));
        assert!(!is_valid_option("user", SERVER));
        assert!(is_valid_option("password", MAPPING));
        assert!(is_valid_option("sslpassword", MAPPING));
        assert!(is_valid_option("sslcert", SERVER) && is_valid_option("sslcert", MAPPING));
        assert!(is_valid_option("sslkey", SERVER) && is_valid_option("sslkey", MAPPING));
        assert!(is_valid_option("gssdelegation", SERVER) && is_valid_option("gssdelegation", MAPPING));
        assert!(is_valid_option("use_remote_estimate", SERVER));
        assert!(is_valid_option("use_remote_estimate", TABLE));
        assert!(is_valid_option("fdw_startup_cost", SERVER));
        assert!(!is_valid_option("fdw_startup_cost", TABLE));
        assert!(is_valid_option("column_name", ATTR));
        assert!(!is_valid_option("column_name", TABLE));
        assert!(is_valid_option("password_required", MAPPING));
        // C's InitPgFdwOptions filters: hidden/overridden libpq entries.
        for gone in ["client_encoding", "fallback_application_name", "replication",
                     "scram_client_key", "scram_server_key", "sslkeylogfile",
                     "oauth_issuer", "oauth_client_id", "oauth_client_secret", "oauth_scope"] {
            assert!(
                !is_valid_option(gone, SERVER) && !is_valid_option(gone, MAPPING),
                "{gone} must be filtered"
            );
        }
        assert!(is_libpq_option("host") && is_libpq_option("sslcert"));
        assert!(!is_libpq_option("schema_name") && !is_libpq_option("extensions"));
    }
}
