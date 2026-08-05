use types_error::{ErrorLevel, PgResult};
use types_startup::AuthToken;

use crate::check::check_role;
use crate::token::make_auth_token;
use crate::token::{copy_auth_token, regcomp_auth_token, regexec_auth_token};
use crate::{
    report_config, token_has_regexp, token_matches, token_matches_insensitive, TokenizedAuthLine,
};

#[derive(Clone, Debug)]
pub struct IdentLine {
    pub linenumber: i32,
    pub usermap: String,
    pub system_user: AuthToken,
    pub pg_user: AuthToken,
}

pub fn parse_ident_line(
    tok_line: &mut TokenizedAuthLine,
    elevel: ErrorLevel,
) -> PgResult<Option<IdentLine>> {
    let line_num = tok_line.line_num;
    let file_name = tok_line.file_name.clone();

    macro_rules! ident_error {
        ($cline:expr, $msg:expr) => {{
            let msg: String = $msg.to_string();
            report_config(elevel, $cline, "parse_ident_line", msg.clone(), None, line_num, &file_name)?;
            tok_line.err_msg = Some(msg);
            return Ok(None);
        }};
    }

    debug_assert!(!tok_line.fields.is_empty());
    let mut field = 0usize;

    // Get the map token (must exist).
    if tok_line.fields[field].len() > 1 {
        ident_error!(2755, "multiple values in ident field");
    }
    let usermap = tok_line.fields[field][0].string.clone();

    // Get the ident user token.
    field += 1;
    if field >= tok_line.fields.len() {
        ident_error!(2761, "missing entry at end of line");
    }
    if tok_line.fields[field].len() > 1 {
        ident_error!(2762, "multiple values in ident field");
    }
    let system_user = copy_auth_token(&tok_line.fields[field][0]);

    // Get the PG rolename token.
    field += 1;
    if field >= tok_line.fields.len() {
        ident_error!(2769, "missing entry at end of line");
    }
    if tok_line.fields[field].len() > 1 {
        ident_error!(2770, "multiple values in ident field");
    }
    let pg_user = copy_auth_token(&tok_line.fields[field][0]);

    let mut system_user = system_user;
    let mut pg_user = pg_user;
    let mut err_msg = None;
    if regcomp_auth_token(&mut system_user, &file_name, line_num, &mut err_msg, elevel)? != 0 {
        tok_line.err_msg = err_msg;
        return Ok(None);
    }
    let mut err_msg = None;
    if regcomp_auth_token(&mut pg_user, &file_name, line_num, &mut err_msg, elevel)? != 0 {
        tok_line.err_msg = err_msg;
        return Ok(None);
    }

    Ok(Some(IdentLine {
        linenumber: line_num,
        usermap,
        system_user,
        pg_user,
    }))
}

// (found, error) out-flags of the C void fn.
pub fn check_ident_usermap(
    ident_line: &IdentLine,
    usermap_name: &str,
    pg_user: &str,
    system_user: &str,
    case_insensitive: bool,
) -> PgResult<(bool, bool)> {
    if ident_line.usermap != usermap_name {
        return Ok((false, false));
    }

    // Get the target role's OID. Note we do not error out for bad role.
    let roleid = acl_seams::get_role_oid::call(pg_user, true)?;

    if token_has_regexp(&ident_line.system_user) {
        return check_ident_usermap_regexp(ident_line, pg_user, system_user, case_insensitive, roleid);
    }

    // Not a regular expression, so make a complete match.
    if case_insensitive {
        if !token_matches_insensitive(&ident_line.system_user, system_user.as_bytes()) {
            return Ok((false, false));
        }
    } else if !token_matches(&ident_line.system_user, system_user.as_bytes()) {
        return Ok((false, false));
    }

    let found = check_role(
        pg_user,
        roleid,
        std::slice::from_ref(&ident_line.pg_user),
        case_insensitive,
    )?;
    Ok((found, false))
}

// check_ident_usermap's regex leg (hba.c:2836): match system_user against the
// compiled regex; substitute the first captured group for \\1 in pg_user
// unless pg_user has special meaning (group/regex). (found, error) out-pair.
fn check_ident_usermap_regexp(
    ident_line: &IdentLine,
    pg_user: &str,
    system_user: &str,
    case_insensitive: bool,
    roleid: types_core::Oid,
) -> PgResult<(bool, bool)> {
    use types_error::{ERRCODE_INVALID_REGULAR_EXPRESSION, LOG};

    let mut matches = [::regex::RegMatch::UNSET; 2];
    match regexec_auth_token(system_user, &ident_line.system_user, &mut matches)? {
        Err(errstr) => {
            elog::ereport(LOG)
                .errcode(ERRCODE_INVALID_REGULAR_EXPRESSION)
                .errmsg(format!(
                    "regular expression match for \"{}\" failed: {errstr}",
                    &ident_line.system_user.string[1..]
                ))
                .finish(crate::loc(2860, "check_ident_usermap"))?;
            return Ok((false, true));
        }
        Ok(false) => return Ok((false, false)),
        Ok(true) => {}
    }

    let pg_tok = &ident_line.pg_user;
    let expanded: AuthToken;
    let check_tok = if !crate::token_is_member_check(pg_tok)
        && !token_has_regexp(pg_tok)
        && pg_tok.string.contains("\\1")
    {
        if matches[1].rm_so < 0 {
            elog::ereport(LOG)
                .errcode(ERRCODE_INVALID_REGULAR_EXPRESSION)
                .errmsg(format!(
                    "regular expression \"{}\" has no subexpressions as requested by backreference in \"{}\"",
                    &ident_line.system_user.string[1..],
                    pg_tok.string
                ))
                .finish(crate::loc(2884, "check_ident_usermap"))?;
            return Ok((false, true));
        }
        // C slices the byte string with the wchar match offsets (single-byte
        // and ASCII coincide; mirror the C behavior).
        let cap = &system_user[matches[1].rm_so as usize..matches[1].rm_eo as usize];
        let expanded_str = pg_tok.string.replacen("\\1", cap, 1);
        expanded = make_auth_token(expanded_str.as_bytes(), true);
        &expanded
    } else {
        pg_tok
    };

    let found = check_role(pg_user, roleid, std::slice::from_ref(check_tok), case_insensitive)?;
    Ok((found, false))
}
