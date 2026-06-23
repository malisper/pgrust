//! The SQL-view fill of `hbafuncs.c`: `get_hba_options`, `fill_hba_line`,
//! `fill_hba_view`, `fill_ident_line`, `fill_ident_view`, plus the seam entries
//! `misc2` consumes (`fill_hba_view` / `fill_ident_view` / `hba_authname`).
//!
//! Ported from `src/backend/utils/adt/hbafuncs.c`. Rows are built as
//! `values[]` / `nulls[]` arrays and inserted through
//! `materialized_srf_putvalues` (the repo's `tuplestore_putvalues` analog).

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use backend_utils_adt_arrayfuncs_seams::construct_text_array_bytes;
use backend_utils_adt_varlena_seams::cstring_to_text_v;
use backend_utils_fmgr_funcapi_seams::materialized_srf_putvalues;
use mcx::{Mcx, PgString};
use types_error::{PgResult, DEBUG3, ERROR};
use types_core::{uaGSS, uaLDAP, uaOAuth, uaRADIUS, uaSSPI, UserAuth};
use types_net::{
    clientCertCA, clientCertOff, ctHost, ctHostGSS, ctHostNoGSS, ctHostNoSSL, ctHostSSL, ctLocal,
    ipCmpAll, ipCmpMask, ipCmpSameHost, ipCmpSameNet, HbaLine,
};
use types_nodes::funcapi::ReturnSetInfo;
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::loaders::{hba_authname, hba_file_name, ident_file_name};
use crate::parse_hba::parse_hba_line;
use crate::parse_ident::parse_ident_line;
use crate::token::{free_auth_file, open_auth_file};
use crate::tokenize::tokenize_auth_file;
use crate::{tok_str, IdentLine, TokenizedAuthLine};

/// Number of columns in `pg_hba_file_rules` (`NUM_PG_HBA_FILE_RULES_ATTS`).
const NUM_PG_HBA_FILE_RULES_ATTS: usize = 11;
/// Number of columns in `pg_ident_file_mappings`
/// (`NUM_PG_IDENT_FILE_MAPPINGS_ATTS`).
const NUM_PG_IDENT_FILE_MAPPINGS_ATTS: usize = 7;

/// Derive the per-call `Mcx` from the `ReturnSetInfo`'s `setDesc` (the
/// multi-call memory context the TupleDesc was built in — the context C's
/// `CStringGetTextDatum` / `heap_form_tuple` palloc in).
fn rsinfo_mcx<'mcx>(rsinfo: &ReturnSetInfo<'mcx>) -> Mcx<'mcx> {
    let tupdesc = rsinfo
        .setDesc
        .as_ref()
        .expect("InitMaterializedSRF establishes rsinfo->setDesc");
    *allocator_api2::boxed::Box::allocator(tupdesc)
}

/// `CStringGetTextDatum(s)` — a `text` `Datum`.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    cstring_to_text_v::call(mcx, s)
}

/// `static ArrayType *get_hba_options(HbaLine *hba)` (hbafuncs.c:51). Build the
/// list of `name=value` option strings on the line; `construct_array_builtin`
/// over `TEXTOID` becomes `text_array_from_strings` at the call site. Empty
/// list => the C `NULL` (no options column value).
fn get_hba_options(hba: &HbaLine) -> Vec<String> {
    let mut options: Vec<String> = Vec::new();

    if hba.auth_method == uaGSS || hba.auth_method == uaSSPI {
        if hba.include_realm {
            options.push("include_realm=true".to_string());
        }
        if let Some(r) = &hba.krb_realm {
            options.push(format!("krb_realm={r}"));
        }
    }

    if let Some(m) = &hba.usermap {
        options.push(format!("map={m}"));
    }

    if hba.clientcert != clientCertOff {
        let v = if hba.clientcert == clientCertCA {
            "verify-ca"
        } else {
            "verify-full"
        };
        options.push(format!("clientcert={v}"));
    }

    if let Some(p) = &hba.pamservice {
        options.push(format!("pamservice={p}"));
    }

    if hba.auth_method == uaLDAP {
        if let Some(v) = &hba.ldapserver {
            options.push(format!("ldapserver={v}"));
        }
        if hba.ldapport != 0 {
            options.push(format!("ldapport={}", hba.ldapport));
        }
        if let Some(v) = &hba.ldapscheme {
            options.push(format!("ldapscheme={v}"));
        }
        if hba.ldaptls {
            options.push("ldaptls=true".to_string());
        }
        if let Some(v) = &hba.ldapprefix {
            options.push(format!("ldapprefix={v}"));
        }
        if let Some(v) = &hba.ldapsuffix {
            options.push(format!("ldapsuffix={v}"));
        }
        if let Some(v) = &hba.ldapbasedn {
            options.push(format!("ldapbasedn={v}"));
        }
        if let Some(v) = &hba.ldapbinddn {
            options.push(format!("ldapbinddn={v}"));
        }
        if let Some(v) = &hba.ldapbindpasswd {
            options.push(format!("ldapbindpasswd={v}"));
        }
        if let Some(v) = &hba.ldapsearchattribute {
            options.push(format!("ldapsearchattribute={v}"));
        }
        if let Some(v) = &hba.ldapsearchfilter {
            options.push(format!("ldapsearchfilter={v}"));
        }
        if hba.ldapscope != 0 {
            options.push(format!("ldapscope={}", hba.ldapscope));
        }
    }

    if hba.auth_method == uaRADIUS {
        if let Some(v) = &hba.radiusservers_s {
            options.push(format!("radiusservers={v}"));
        }
        if let Some(v) = &hba.radiussecrets_s {
            options.push(format!("radiussecrets={v}"));
        }
        if let Some(v) = &hba.radiusidentifiers_s {
            options.push(format!("radiusidentifiers={v}"));
        }
        if let Some(v) = &hba.radiusports_s {
            options.push(format!("radiusports={v}"));
        }
    }

    if hba.auth_method == uaOAuth {
        if let Some(v) = &hba.oauth_issuer {
            options.push(format!("issuer={v}"));
        }
        if let Some(v) = &hba.oauth_scope {
            options.push(format!("scope={v}"));
        }
        if let Some(v) = &hba.oauth_validator {
            options.push(format!("validator={v}"));
        }
        if hba.oauth_skip_usermap {
            options.push("delegate_ident_mapping=true".to_string());
        }
    }

    options
}

/// `construct_array_builtin(elems, n, TEXTOID)` — build a `text[]` array `Datum`
/// from owned strings (the C `strlist_to_textarray` / `construct_array_builtin`
/// over `TEXTOID`). A `text[]` is a by-reference (varlena) type, so the array
/// image must ride the by-reference Datum lane — the form
/// `materialized_srf_putvalues` reads via `as_ref_bytes` when it lowers the row
/// into the tuplestore. `construct_text_array_bytes` hands back the flat array
/// varlena byte image for exactly this lowering (the same byref pattern
/// `show_all_settings` uses for its `enumvals` text[] column); carrying it on the
/// by-value pointer arm instead made `as_ref_bytes` panic
/// (`called on a by-value attribute`).
fn text_array_datum<'mcx>(mcx: Mcx<'mcx>, elems: &[String]) -> PgResult<Datum<'mcx>> {
    let refs: Vec<&str> = elems.iter().map(|s| s.as_str()).collect();
    let image = construct_text_array_bytes::call(mcx, &refs)?;
    Datum::from_byref_bytes_in(mcx, &image)
}

/// Build a `text[]` array `Datum` of the token strings (the C
/// `strlist_to_textarray` over the flattened AuthToken `string`s).
fn token_strings_array<'mcx>(
    mcx: Mcx<'mcx>,
    tokens: &[types_net::AuthToken],
) -> PgResult<Datum<'mcx>> {
    let strs: Vec<String> = tokens
        .iter()
        .map(|t| String::from_utf8_lossy(tok_str(t)).into_owned())
        .collect();
    text_array_datum(mcx, &strs)
}

/// Render an `HbaLine` `addr`/`mask` byte buffer to its numeric host string
/// (`pg_getnameinfo_all(NI_NUMERICHOST)` + `clean_ipv6_addr`). `salen` 0 => no
/// string (the C `if (hba->addrlen > 0)` guard).
fn numeric_host(addr: &[u8; 128], salen: i32) -> Option<String> {
    if salen <= 0 {
        return None;
    }
    let sa = types_net::SockAddr { addr: *addr, salen: salen as u32 };
    let mut buffer = String::new();
    // Note: if pg_getnameinfo_all fails, it'll set buffer to "???", which we
    // want to return. (common_ip writes "???" on failure too.)
    if common_ip::pg_getnameinfo_all(&sa, Some(&mut buffer), None, libc::NI_NUMERICHOST) == 0 {
        // clean_ipv6_addr(addr.ss_family, buffer)
        let fam = crate::matchers::ss_family(&sa);
        let mut bytes = buffer.into_bytes();
        backend_utils_adt_network::clean_ipv6_addr(fam, &mut bytes);
        buffer = String::from_utf8_lossy(&bytes).into_owned();
    }
    Some(buffer)
}

/// `static void fill_hba_line(...)` (hbafuncs.c:201). Build one row of the
/// `pg_hba_file_rules` view and add it to the tuplestore.
fn fill_hba_line<'mcx>(
    rsinfo: &mut ReturnSetInfo<'mcx>,
    mcx: Mcx<'mcx>,
    rule_number: i32,
    filename: &str,
    lineno: i32,
    hba: Option<&HbaLine>,
    err_msg: Option<&str>,
) -> PgResult<()> {
    let mut values: Vec<Datum<'mcx>> = vec![Datum::null(); NUM_PG_HBA_FILE_RULES_ATTS];
    let mut nulls = [false; NUM_PG_HBA_FILE_RULES_ATTS];
    let mut index = 0usize;

    // rule_number, nothing on error.
    if err_msg.is_some() {
        nulls[index] = true;
        index += 1;
    } else {
        values[index] = Datum::from_i32(rule_number);
        index += 1;
    }

    // file_name.
    values[index] = text_datum(mcx, filename)?;
    index += 1;

    // line_number.
    values[index] = Datum::from_i32(lineno);
    index += 1;

    if let Some(hba) = hba {
        // type.
        let typestr: Option<&str> = match hba.conntype {
            c if c == ctLocal => Some("local"),
            c if c == ctHost => Some("host"),
            c if c == ctHostSSL => Some("hostssl"),
            c if c == ctHostNoSSL => Some("hostnossl"),
            c if c == ctHostGSS => Some("hostgssenc"),
            c if c == ctHostNoGSS => Some("hostnogssenc"),
            _ => None,
        };
        if let Some(t) = typestr {
            values[index] = text_datum(mcx, t)?;
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }

        // database.
        if !hba.databases.is_empty() {
            values[index] = token_strings_array(mcx, &hba.databases)?;
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }

        // user.
        if !hba.roles.is_empty() {
            values[index] = token_strings_array(mcx, &hba.roles)?;
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }

        // address and netmask.
        let mut addrstr: Option<String> = None;
        let mut maskstr: Option<String> = None;
        match hba.ip_cmp_method {
            m if m == ipCmpMask => {
                if let Some(h) = &hba.hostname {
                    addrstr = Some(h.clone());
                } else {
                    if hba.addrlen > 0 {
                        addrstr = numeric_host(&hba.addr, hba.addrlen);
                    }
                    if hba.masklen > 0 {
                        maskstr = numeric_host(&hba.mask, hba.masklen);
                    }
                }
            }
            m if m == ipCmpAll => addrstr = Some("all".to_string()),
            m if m == ipCmpSameHost => addrstr = Some("samehost".to_string()),
            m if m == ipCmpSameNet => addrstr = Some("samenet".to_string()),
            _ => {}
        }
        if let Some(a) = addrstr {
            values[index] = text_datum(mcx, &a)?;
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }
        if let Some(m) = maskstr {
            values[index] = text_datum(mcx, &m)?;
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }

        // auth_method.
        values[index] = text_datum(mcx, hba_authname(hba.auth_method))?;
        index += 1;

        // options.
        let opts = get_hba_options(hba);
        if !opts.is_empty() {
            values[index] = text_array_datum(mcx, &opts)?;
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }
    } else {
        // no parsing result, so set relevant fields to nulls
        // (memset(&nulls[3], true, (ATTS - 4))).
        for n in nulls.iter_mut().take(NUM_PG_HBA_FILE_RULES_ATTS - 1).skip(3) {
            *n = true;
        }
    }

    // error.
    if let Some(e) = err_msg {
        values[NUM_PG_HBA_FILE_RULES_ATTS - 1] = text_datum(mcx, e)?;
    } else {
        nulls[NUM_PG_HBA_FILE_RULES_ATTS - 1] = true;
    }

    let _ = index;
    materialized_srf_putvalues::call(rsinfo, &values, &nulls)
}

/// `static void fill_hba_view(...)` (hbafuncs.c:392).
fn fill_hba_view<'mcx>(rsinfo: &mut ReturnSetInfo<'mcx>) -> PgResult<()> {
    let mcx = rsinfo_mcx(rsinfo);
    let hba_file = hba_file_name();

    // In the unlikely event we can't open pg_hba.conf, throw an error.
    let mut open_err = None;
    let file = match open_auth_file(&hba_file, ERROR, 0, &mut open_err)? {
        Some(f) => f,
        None => return Ok(()), // open_auth_file at ERROR raised on Err already
    };

    let mut hba_lines: Vec<TokenizedAuthLine> = Vec::new();
    tokenize_auth_file(&hba_file, &file, &mut hba_lines, DEBUG3, 0)?;

    let mut rule_number = 0i32;
    for tok_line in hba_lines.iter_mut() {
        let mut hbaline: Option<HbaLine> = None;
        // don't parse lines that already have errors
        if tok_line.err_msg.is_none() {
            hbaline = parse_hba_line(tok_line, DEBUG3)?;
        }
        // No error, set a new rule number.
        if tok_line.err_msg.is_none() {
            rule_number += 1;
        }

        let file_name = tok_line.file_name.clone();
        let line_num = tok_line.line_num;
        let err = tok_line.err_msg.clone();
        fill_hba_line(
            rsinfo,
            mcx,
            rule_number,
            &file_name,
            line_num,
            hbaline.as_ref(),
            err.as_deref(),
        )?;
    }

    free_auth_file(file, 0);
    Ok(())
}

/// `static void fill_ident_line(...)` (hbafuncs.c:486).
fn fill_ident_line<'mcx>(
    rsinfo: &mut ReturnSetInfo<'mcx>,
    mcx: Mcx<'mcx>,
    map_number: i32,
    filename: &str,
    lineno: i32,
    ident: Option<&IdentLine>,
    err_msg: Option<&str>,
) -> PgResult<()> {
    let mut values: Vec<Datum<'mcx>> = vec![Datum::null(); NUM_PG_IDENT_FILE_MAPPINGS_ATTS];
    let mut nulls = [false; NUM_PG_IDENT_FILE_MAPPINGS_ATTS];
    let mut index = 0usize;

    // map_number, nothing on error.
    if err_msg.is_some() {
        nulls[index] = true;
        index += 1;
    } else {
        values[index] = Datum::from_i32(map_number);
        index += 1;
    }

    // file_name.
    values[index] = text_datum(mcx, filename)?;
    index += 1;
    // line_number.
    values[index] = Datum::from_i32(lineno);
    index += 1;

    if let Some(ident) = ident {
        values[index] = text_datum(mcx, &ident.usermap)?;
        index += 1;
        values[index] =
            text_datum(mcx, &String::from_utf8_lossy(tok_str(&ident.system_user)))?;
        index += 1;
        values[index] = text_datum(mcx, &String::from_utf8_lossy(tok_str(&ident.pg_user)))?;
        index += 1;
    } else {
        for n in nulls
            .iter_mut()
            .take(NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1)
            .skip(3)
        {
            *n = true;
        }
    }

    // error.
    if let Some(e) = err_msg {
        values[NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1] = text_datum(mcx, e)?;
    } else {
        nulls[NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1] = true;
    }

    let _ = index;
    materialized_srf_putvalues::call(rsinfo, &values, &nulls)
}

/// `static void fill_ident_view(...)` (hbafuncs.c:539).
fn fill_ident_view<'mcx>(rsinfo: &mut ReturnSetInfo<'mcx>) -> PgResult<()> {
    let mcx = rsinfo_mcx(rsinfo);
    let ident_file = ident_file_name();

    let mut open_err = None;
    let file = match open_auth_file(&ident_file, ERROR, 0, &mut open_err)? {
        Some(f) => f,
        None => return Ok(()),
    };

    let mut ident_lines: Vec<TokenizedAuthLine> = Vec::new();
    tokenize_auth_file(&ident_file, &file, &mut ident_lines, DEBUG3, 0)?;

    let mut map_number = 0i32;
    for tok_line in ident_lines.iter_mut() {
        let mut identline: Option<IdentLine> = None;
        if tok_line.err_msg.is_none() {
            identline = parse_ident_line(tok_line, DEBUG3)?;
        }
        if tok_line.err_msg.is_none() {
            map_number += 1;
        }

        let file_name = tok_line.file_name.clone();
        let line_num = tok_line.line_num;
        let err = tok_line.err_msg.clone();
        fill_ident_line(
            rsinfo,
            mcx,
            map_number,
            &file_name,
            line_num,
            identline.as_ref(),
            err.as_deref(),
        )?;
    }

    free_auth_file(file, 0);
    Ok(())
}

// ---------------------------------------------------------------------------
// Seam entries consumed by misc2.
// ---------------------------------------------------------------------------

/// `fill_hba_view(rsinfo)` seam.
pub(crate) fn fill_hba_view_entry<'mcx>(rsinfo: &mut ReturnSetInfo<'mcx>) -> PgResult<()> {
    fill_hba_view(rsinfo)
}

/// `fill_ident_view(rsinfo)` seam.
pub(crate) fn fill_ident_view_entry<'mcx>(rsinfo: &mut ReturnSetInfo<'mcx>) -> PgResult<()> {
    fill_ident_view(rsinfo)
}

/// `hba_authname(mcx)` seam — read `MyClientConnectionInfo.auth_method` and
/// return its name as a `PgString`.
pub(crate) fn hba_authname_entry<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgString<'mcx>> {
    let info = backend_utils_init_miscinit::client_connection_info();
    let method: UserAuth = info.auth_method;
    PgString::from_str_in(hba_authname(method), mcx)
}
