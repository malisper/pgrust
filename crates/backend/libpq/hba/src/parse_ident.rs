use types_error::{ErrorLevel, PgResult};
use types_startup::AuthToken;

use crate::check::check_role;
use crate::token::{copy_auth_token, regcomp_auth_token};
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

    if regcomp_auth_token(&system_user, &file_name, line_num)? != 0 {
        return Ok(None);
    }
    if regcomp_auth_token(&pg_user, &file_name, line_num)? != 0 {
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
        unreachable!("regex auth tokens panic at parse");
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
