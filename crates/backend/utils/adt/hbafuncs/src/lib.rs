//! hbafuncs.c: pg_hba_file_rules and pg_ident_file_mappings SRFs. Runs the
//! tokenizer/parser fresh per call (as C does) so an in-progress edit of the
//! auth files is reflected without a reload.

use datum::Datum;
use funcapi::{InitMaterializedSRF, MaterializedSRF};
use hba::TokenizedAuthLine;
use mcx::Mcx;
use types_core::catalog::TEXTOID;
use types_error::{PgResult, ERROR};
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_startup::{clientCertCA, clientCertOff, AuthToken, HbaLine};

pub static HBAFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    srf(3401, "pg_hba_file_rules", fc_pg_hba_file_rules),
    srf(6250, "pg_ident_file_mappings", fc_pg_ident_file_mappings),
];

const fn srf(foid: types_core::Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 0, strict: true, retset: true, func }
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

fn text_array_datum(mcx: Mcx<'_>, strings: &[&str]) -> PgResult<Datum> {
    let mut datums: Vec<Datum> = Vec::with_capacity(strings.len());
    for s in strings {
        datums.push(text_datum(mcx, s)?);
    }
    let img = arrayfuncs::construct_array(mcx, &datums, TEXTOID, -1, false, b'i')?;
    Ok(image_datum(img))
}

fn image_datum(img: mcx::PgVec<'_, u8>) -> Datum {
    let d = Datum::from_usize(img.as_ptr() as usize);
    core::mem::forget(img);
    d
}

// network.c:2060 clean_ipv6_addr: strip a trailing '%zone' suffix so the
// string round-trips through inet_in.
fn clean_ipv6_addr(family: i32, addr: &mut String) {
    if family == ip::sys::AF_INET6 {
        if let Some(pos) = addr.find('%') {
            addr.truncate(pos);
        }
    }
}

fn numeric_host(sa: &ip::SockAddr) -> String {
    let mut node = String::new();
    ip::pg_getnameinfo_all(sa, Some(&mut node), None, ip::sys::NI_NUMERICHOST);
    clean_ipv6_addr(ip::sockaddr_family(sa), &mut node);
    node
}

// get_hba_options (hbafuncs.c:52). GSS/SSPI/PAM/BSD/LDAP/RADIUS/OAuth are
// rejected at parse time in this build (hba::parse_hba_line), so HbaLine
// carries no fields for them and those option branches are unreachable.
// Config-file SRF: per-call frequency, not per-row — bare Vec/String is the
// same cost class hba::HbaLine already uses for its own fields.
fn hba_options_strings(hba: &HbaLine) -> Vec<String> {
    let mut opts: Vec<String> = Vec::new();

    if let Some(usermap) = &hba.usermap {
        opts.push(format!("map={usermap}"));
    }
    if hba.clientcert != clientCertOff {
        let mode = if hba.clientcert == clientCertCA { "verify-ca" } else { "verify-full" };
        opts.push(format!("clientcert={mode}"));
    }

    opts
}

fn get_hba_options(mcx: Mcx<'_>, hba: &HbaLine) -> PgResult<Option<Datum>> {
    let opts = hba_options_strings(hba);
    if opts.is_empty() {
        return Ok(None);
    }
    let refs: Vec<&str> = opts.iter().map(String::as_str).collect();
    Ok(Some(text_array_datum(mcx, &refs)?))
}

// fill_hba_line's `typestr` computation (hbafuncs.c:238-260).
fn hba_typestr(conntype: types_startup::ConnType) -> &'static str {
    match conntype {
        types_startup::ctLocal => "local",
        types_startup::ctHost => "host",
        types_startup::ctHostSSL => "hostssl",
        types_startup::ctHostNoSSL => "hostnossl",
        types_startup::ctHostGSS => "hostgssenc",
        types_startup::ctHostNoGSS => "hostnogssenc",
        other => panic!("fill_hba_line: unknown conntype {other}"),
    }
}

// fill_hba_line's `addrstr`/`maskstr` computation (hbafuncs.c:307-360).
fn hba_addr_mask(hba: &HbaLine) -> (Option<String>, Option<String>) {
    match hba.ip_cmp_method {
        types_startup::ipCmpMask => {
            if let Some(hostname) = &hba.hostname {
                (Some(hostname.clone()), None)
            } else {
                let addr = (hba.addr.salen > 0).then(|| numeric_host(&hba.addr));
                let mask = (hba.mask.salen > 0).then(|| numeric_host(&hba.mask));
                (addr, mask)
            }
        }
        types_startup::ipCmpAll => (Some("all".to_string()), None),
        types_startup::ipCmpSameHost => (Some("samehost".to_string()), None),
        types_startup::ipCmpSameNet => (Some("samenet".to_string()), None),
        other => panic!("fill_hba_line: unknown ip_cmp_method {other}"),
    }
}

const NUM_PG_HBA_FILE_RULES_ATTS: usize = 11;

#[allow(clippy::too_many_arguments)]
fn fill_hba_line(
    srf: &mut MaterializedSRF<'_>,
    mcx: Mcx<'_>,
    rule_number: i32,
    filename: &str,
    lineno: i32,
    hba: Option<&HbaLine>,
    err_msg: Option<&str>,
) -> PgResult<()> {
    let mut values = [Datum::null(); NUM_PG_HBA_FILE_RULES_ATTS];
    let mut nulls = [false; NUM_PG_HBA_FILE_RULES_ATTS];
    let mut i = 0;

    if err_msg.is_some() {
        nulls[i] = true;
    } else {
        values[i] = Datum::from_i32(rule_number);
    }
    i += 1;

    values[i] = text_datum(mcx, filename)?;
    i += 1;
    values[i] = Datum::from_i32(lineno);
    i += 1;

    if let Some(hba) = hba {
        values[i] = text_datum(mcx, hba_typestr(hba.conntype))?;
        i += 1;

        if hba.databases.is_empty() {
            nulls[i] = true;
        } else {
            values[i] = auth_token_array(mcx, &hba.databases)?;
        }
        i += 1;

        if hba.roles.is_empty() {
            nulls[i] = true;
        } else {
            values[i] = auth_token_array(mcx, &hba.roles)?;
        }
        i += 1;

        let (addrstr, maskstr) = hba_addr_mask(hba);
        match addrstr {
            Some(s) => values[i] = text_datum(mcx, &s)?,
            None => nulls[i] = true,
        }
        i += 1;
        match maskstr {
            Some(s) => values[i] = text_datum(mcx, &s)?,
            None => nulls[i] = true,
        }
        i += 1;

        values[i] = text_datum(mcx, hba::hba_authname(hba.auth_method))?;
        i += 1;

        match get_hba_options(mcx, hba)? {
            Some(d) => values[i] = d,
            None => nulls[i] = true,
        }
        i += 1;
    } else {
        for slot in nulls.iter_mut().take(NUM_PG_HBA_FILE_RULES_ATTS - 1).skip(3) {
            *slot = true;
        }
        i = NUM_PG_HBA_FILE_RULES_ATTS - 1;
    }

    if let Some(msg) = err_msg {
        values[i] = text_datum(mcx, msg)?;
    } else {
        nulls[i] = true;
    }

    srf.putvalues(&values, &nulls)
}

fn auth_token_array(mcx: Mcx<'_>, tokens: &[AuthToken]) -> PgResult<Datum> {
    let refs: Vec<&str> = tokens.iter().map(|t| t.string.as_str()).collect();
    text_array_datum(mcx, &refs)
}

fn fill_hba_view(srf: &mut MaterializedSRF<'_>, mcx: Mcx<'_>) -> PgResult<()> {
    let filename = hba::hba_file_name();
    let mut open_err = None;
    let Some(file) = hba::open_auth_file(&filename, ERROR, 0, &mut open_err)? else {
        unreachable!("open_auth_file at ERROR level always errors out via ?, never returns None");
    };

    let mut hba_lines: Vec<TokenizedAuthLine> = Vec::new();
    hba::tokenize_auth_file(&filename, &file, &mut hba_lines, types_error::DEBUG3, 0)?;

    let mut rule_number = 0;
    for tok_line in hba_lines.iter_mut() {
        let hbaline = if tok_line.err_msg.is_none() {
            hba::parse_hba_line(tok_line, types_error::DEBUG3)?
        } else {
            None
        };
        if tok_line.err_msg.is_none() {
            rule_number += 1;
        }
        fill_hba_line(
            srf,
            mcx,
            rule_number,
            &tok_line.file_name,
            tok_line.line_num,
            hbaline.as_ref(),
            tok_line.err_msg.as_deref(),
        )?;
    }

    hba::free_auth_file(file, 0);
    Ok(())
}

pub fn fc_pg_hba_file_rules(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_hba_file_rules: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    fill_hba_view(&mut srf, mcx)?;
    Ok(srf.finish(fcinfo))
}

const NUM_PG_IDENT_FILE_MAPPINGS_ATTS: usize = 7;

fn fill_ident_line(
    srf: &mut MaterializedSRF<'_>,
    mcx: Mcx<'_>,
    map_number: i32,
    filename: &str,
    lineno: i32,
    ident: Option<&hba::IdentLine>,
    err_msg: Option<&str>,
) -> PgResult<()> {
    let mut values = [Datum::null(); NUM_PG_IDENT_FILE_MAPPINGS_ATTS];
    let mut nulls = [false; NUM_PG_IDENT_FILE_MAPPINGS_ATTS];
    let mut i = 0;

    if err_msg.is_some() {
        nulls[i] = true;
    } else {
        values[i] = Datum::from_i32(map_number);
    }
    i += 1;

    values[i] = text_datum(mcx, filename)?;
    i += 1;
    values[i] = Datum::from_i32(lineno);
    i += 1;

    if let Some(ident) = ident {
        values[i] = text_datum(mcx, &ident.usermap)?;
        i += 1;
        values[i] = text_datum(mcx, &ident.system_user.string)?;
        i += 1;
        values[i] = text_datum(mcx, &ident.pg_user.string)?;
        i += 1;
    } else {
        for slot in nulls.iter_mut().take(NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1).skip(3) {
            *slot = true;
        }
        i = NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1;
    }

    if let Some(msg) = err_msg {
        values[i] = text_datum(mcx, msg)?;
    } else {
        nulls[i] = true;
    }

    srf.putvalues(&values, &nulls)
}

fn fill_ident_view(srf: &mut MaterializedSRF<'_>, mcx: Mcx<'_>) -> PgResult<()> {
    let filename = hba::ident_file_name();
    let mut open_err = None;
    let Some(file) = hba::open_auth_file(&filename, ERROR, 0, &mut open_err)? else {
        unreachable!("open_auth_file at ERROR level always errors out via ?, never returns None");
    };

    let mut ident_lines: Vec<TokenizedAuthLine> = Vec::new();
    hba::tokenize_auth_file(&filename, &file, &mut ident_lines, types_error::DEBUG3, 0)?;

    let mut map_number = 0;
    for tok_line in ident_lines.iter_mut() {
        let identline = if tok_line.err_msg.is_none() {
            hba::parse_ident_line(tok_line, types_error::DEBUG3)?
        } else {
            None
        };
        if tok_line.err_msg.is_none() {
            map_number += 1;
        }
        fill_ident_line(
            srf,
            mcx,
            map_number,
            &tok_line.file_name,
            tok_line.line_num,
            identline.as_ref(),
            tok_line.err_msg.as_deref(),
        )?;
    }

    hba::free_auth_file(file, 0);
    Ok(())
}

pub fn fc_pg_ident_file_mappings(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_ident_file_mappings: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    fill_ident_view(&mut srf, mcx)?;
    Ok(srf.finish(fcinfo))
}

#[cfg(test)]
mod tests;
