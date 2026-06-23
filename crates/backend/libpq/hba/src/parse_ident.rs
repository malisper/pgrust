//! The `pg_ident.conf` parser/matcher of `hba.c`: `parse_ident_line` (parse one
//! mapping line) and `check_ident_usermap` (match a mapping against a
//! map/pg_user/system_user triple, including the regex `\1` back-reference
//! substitution).
//!
//! Ported from `src/backend/libpq/hba.c` (`parse_ident_line` @2750,
//! `check_ident_usermap` @2818).

use acl_seams as acl;
use ::types_error::{ErrorLevel, PgResult};
use ::net::AuthToken;

use crate::matchers::check_role;
use crate::token::{copy_auth_token, free_auth_token, make_auth_token, regcomp_auth_token, regexec_auth_token};
use crate::{
    report_config, tok_str, token_has_regexp, token_is_member_check, token_matches,
    token_matches_insensitive, IdentLine, TokenizedAuthLine, REG_NOMATCH,
};

/// `IdentLine *parse_ident_line(TokenizedAuthLine *tok_line, int elevel)`
/// (hba.c:2750).
pub fn parse_ident_line(
    tok_line: &mut TokenizedAuthLine,
    elevel: ErrorLevel,
) -> PgResult<Option<IdentLine>> {
    let line_num = tok_line.line_num;
    let file_name = tok_line.file_name.clone();

    debug_assert!(!tok_line.fields.is_empty());
    let mut field = 0usize;

    // parsedline = palloc0(sizeof(IdentLine)); parsedline->linenumber = line_num;
    let mut usermap: String;
    let system_user: AuthToken;
    let pg_user: AuthToken;

    // Get the map token (must exist).
    {
        let tokens = &tok_line.fields[field];
        if tokens.len() > 1 {
            ident_multi_value(elevel, line_num, &file_name, &mut tok_line.err_msg)?;
            return Ok(None);
        }
        usermap = String::from_utf8_lossy(tok_str(&tokens[0])).into_owned();
    }

    // Get the ident user token.
    field += 1;
    if field >= tok_line.fields.len() {
        ident_field_absent(elevel, line_num, &file_name, &mut tok_line.err_msg)?;
        return Ok(None);
    }
    {
        let tokens = &tok_line.fields[field];
        if tokens.len() > 1 {
            ident_multi_value(elevel, line_num, &file_name, &mut tok_line.err_msg)?;
            return Ok(None);
        }
        system_user = copy_auth_token(&tokens[0]);
    }

    // Get the PG rolename token.
    field += 1;
    if field >= tok_line.fields.len() {
        ident_field_absent(elevel, line_num, &file_name, &mut tok_line.err_msg)?;
        return Ok(None);
    }
    {
        let tokens = &tok_line.fields[field];
        if tokens.len() > 1 {
            ident_multi_value(elevel, line_num, &file_name, &mut tok_line.err_msg)?;
            return Ok(None);
        }
        pg_user = copy_auth_token(&tokens[0]);
    }

    let mut parsedline = IdentLine {
        linenumber: line_num,
        usermap: core::mem::take(&mut usermap),
        system_user,
        pg_user,
    };

    // Compile a regex from the user tokens, if necessary.
    if regcomp_auth_token(
        &mut parsedline.system_user,
        &file_name,
        line_num,
        &mut tok_line.err_msg,
        elevel,
    )? != 0
    {
        return Ok(None);
    }
    if regcomp_auth_token(
        &mut parsedline.pg_user,
        &file_name,
        line_num,
        &mut tok_line.err_msg,
        elevel,
    )? != 0
    {
        return Ok(None);
    }

    Ok(Some(parsedline))
}

/// `static void check_ident_usermap(IdentLine *identLine, const char
/// *usermap_name, const char *pg_user, const char *system_user, bool
/// case_insensitive, bool *found_p, bool *error_p)` (hba.c:2818).
///
/// Returns `(found, error)`.
pub(crate) fn check_ident_usermap(
    ident_line: &IdentLine,
    usermap_name: &[u8],
    pg_user: &[u8],
    system_user: &[u8],
    case_insensitive: bool,
) -> PgResult<(bool, bool)> {
    let mut found_p = false;
    let mut error_p = false;

    // if (strcmp(identLine->usermap, usermap_name) != 0) return;
    if ident_line.usermap.as_bytes() != usermap_name {
        return Ok((found_p, error_p));
    }

    // roleid = get_role_oid(pg_user, true);
    let pg_user_str = String::from_utf8_lossy(pg_user);
    let roleid = acl::get_role_oid::call(&pg_user_str, true)?;

    if token_has_regexp(&ident_line.system_user) {
        // Process the system username as a regex returning one match, replaced
        // for \1 in the database username string, if present.
        let (r, matches, errstr) = regexec_auth_token(system_user, &ident_line.system_user, 2)?;
        if r != 0 {
            if r != REG_NOMATCH {
                // REG_NOMATCH is not an error, everything else is.
                let sys_pat =
                    String::from_utf8_lossy(&tok_str(&ident_line.system_user)[1..]).into_owned();
                let msg = format!(
                    "regular expression match for \"{sys_pat}\" failed: {}",
                    errstr.unwrap_or_default()
                );
                crate::report_plain(
                    ::types_error::LOG,
                    "check_ident_usermap",
                    ::types_error::ERRCODE_INVALID_REGULAR_EXPRESSION,
                    msg,
                )?;
                error_p = true;
            }
            return Ok((found_p, error_p));
        }

        // Replace \1 with the first captured group unless pg_user already has a
        // special meaning (group membership or a regexp-based check).
        let pg_user_str_bytes = tok_str(&ident_line.pg_user);
        let ofs = if !token_is_member_check(&ident_line.pg_user)
            && !token_has_regexp(&ident_line.pg_user)
        {
            find_subslice(pg_user_str_bytes, b"\\1")
        } else {
            None
        };

        let mut expanded_pg_user_token: Option<AuthToken> = None;

        if let Some(offset) = ofs {
            // if (matches[1].rm_so < 0) { error }
            if matches[1].rm_so < 0 {
                let sys_pat =
                    String::from_utf8_lossy(&tok_str(&ident_line.system_user)[1..]).into_owned();
                let pg_str = String::from_utf8_lossy(pg_user_str_bytes).into_owned();
                let msg = format!(
                    "regular expression \"{sys_pat}\" has no subexpressions as requested by backreference in \"{pg_str}\""
                );
                crate::report_plain(
                    ::types_error::LOG,
                    "check_ident_usermap",
                    ::types_error::ERRCODE_INVALID_REGULAR_EXPRESSION,
                    msg,
                )?;
                error_p = true;
                return Ok((found_p, error_p));
            }

            // expanded_pg_user = pg_user[..offset] + system_user[rm_so..rm_eo] + pg_user[offset+2..]
            let so = matches[1].rm_so as usize;
            let eo = matches[1].rm_eo as usize;
            let mut expanded: Vec<u8> = Vec::new();
            expanded.extend_from_slice(&pg_user_str_bytes[..offset]);
            expanded.extend_from_slice(&system_user[so..eo]);
            expanded.extend_from_slice(&pg_user_str_bytes[offset + 2..]);

            // Mark the token as quoted, so it is compared literally.
            expanded_pg_user_token = Some(make_auth_token(&expanded, true));
        }

        // *found_p = check_role(pg_user, roleid, list_make1(expanded_or_pg_user), ci);
        match expanded_pg_user_token {
            Some(mut token) => {
                found_p = check_role(
                    pg_user,
                    roleid,
                    core::slice::from_ref(&token),
                    case_insensitive,
                )?;
                free_auth_token(&mut token);
            }
            None => {
                found_p = check_role(
                    pg_user,
                    roleid,
                    core::slice::from_ref(&ident_line.pg_user),
                    case_insensitive,
                )?;
            }
        }

        Ok((found_p, error_p))
    } else {
        // Not a regex, so make a complete match.
        if case_insensitive {
            if !token_matches_insensitive(&ident_line.system_user, system_user) {
                return Ok((found_p, error_p));
            }
        } else if !token_matches(&ident_line.system_user, system_user) {
            return Ok((found_p, error_p));
        }

        found_p = check_role(
            pg_user,
            roleid,
            core::slice::from_ref(&ident_line.pg_user),
            case_insensitive,
        )?;
        Ok((found_p, error_p))
    }
}

/// `IDENT_FIELD_ABSENT(field)` (hba.c:1288).
fn ident_field_absent(
    elevel: ErrorLevel,
    line_num: i32,
    file_name: &str,
    err_msg: &mut Option<String>,
) -> PgResult<()> {
    report_config(
        elevel,
        "parse_ident_line",
        "missing entry at end of line".to_string(),
        None,
        line_num,
        file_name,
    )?;
    *err_msg = Some("missing entry at end of line".to_string());
    Ok(())
}

/// `IDENT_MULTI_VALUE(tokens)` (hba.c:1301).
fn ident_multi_value(
    elevel: ErrorLevel,
    line_num: i32,
    file_name: &str,
    err_msg: &mut Option<String>,
) -> PgResult<()> {
    report_config(
        elevel,
        "parse_ident_line",
        "multiple values in ident field".to_string(),
        None,
        line_num,
        file_name,
    )?;
    *err_msg = Some("multiple values in ident field".to_string());
    Ok(())
}

/// `strstr(haystack, needle)` byte semantics.
#[inline]
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}
