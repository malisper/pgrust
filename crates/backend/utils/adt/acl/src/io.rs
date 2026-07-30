use cache_syscache::cacheinfo::AUTHOID;
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use mcx::{vec_append_bytes, Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NAME_TOO_LONG,
    ERRCODE_UNDEFINED_OBJECT,
};
use types_fmgr::ErrorSaveNode;

// ereturn against the fmgr ErrorSaveNode: soft path records + returns None.
fn ereturn_soft<T>(escontext: Option<&mut ErrorSaveNode>, err: PgError) -> PgResult<Option<T>> {
    match escontext {
        Some(n) => {
            if n.ctx.details_wanted() {
                n.ctx.save(err);
            } else {
                n.ctx.mark_error_occurred();
            }
            Ok(None)
        }
        None => Err(Box::new(err)),
    }
}

use crate::membership::get_role_oid;
use crate::{
    aclitem_get_goptions, aclitem_get_privs, aclitem_set_privs_goptions, AclItem, ACL_ID_PUBLIC,
    ACL_NO_RIGHTS,
};

pub const ACL_ALL_RIGHTS_STR: &[u8] = b"arwdDxtXUCTcsAm";
const NAMEDATALEN: usize = 64;
const BOOTSTRAP_SUPERUSERID: u32 = 10;
const ANUM_PG_AUTHID_ROLNAME: i32 = 2;

#[inline]
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

// High-bit-set bytes pass unquoted only on the parse side (dump compat).
#[inline]
fn is_safe_acl_char(c: u8, is_getid: bool) -> bool {
    if c & 0x80 != 0 {
        return is_getid;
    }
    c.is_ascii_alphanumeric() || c == b'_'
}

fn getid<'a>(
    s: &'a [u8],
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Option<(&'a [u8], Vec<u8>)>> {
    let mut n: Vec<u8> = Vec::new();
    let mut in_quotes = false;
    let mut i = 0usize;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    while i < s.len() && (in_quotes || s[i] == b'"' || is_safe_acl_char(s[i], true)) {
        if s[i] == b'"' {
            if !in_quotes {
                in_quotes = true;
                i += 1;
                continue;
            }
            if i + 1 >= s.len() || s[i + 1] != b'"' {
                in_quotes = false;
                i += 1;
                continue;
            }
            i += 1;
        }
        if n.len() >= NAMEDATALEN - 1 {
            return ereturn_soft(
                escontext,
                PgError::error("identifier too long")
                    .with_sqlstate(ERRCODE_NAME_TOO_LONG)
                    .with_detail(format!(
                        "Identifier must be less than {NAMEDATALEN} characters."
                    )),
            );
        }
        n.push(s[i]);
        i += 1;
    }
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    Ok(Some((&s[i..], n)))
}

fn putid(p: &mut Vec<u8>, s: &[u8]) {
    let safe = s.iter().all(|&c| is_safe_acl_char(c, false));
    if !safe {
        p.push(b'"');
    }
    for &c in s {
        if c == b'"' {
            p.push(b'"');
        }
        p.push(c);
    }
    if !safe {
        p.push(b'"');
    }
}

#[inline]
fn first(s: &[u8]) -> u8 {
    s.first().copied().unwrap_or(0)
}

fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn priv_for_char(c: u8) -> Option<u64> {
    ACL_ALL_RIGHTS_STR
        .iter()
        .position(|&r| r == c)
        .map(|i| 1u64 << i)
}

pub fn aclparse<'a>(
    s: &'a [u8],
    aip: &mut AclItem,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Option<&'a [u8]>> {
    let Some((mut s, mut name)) = getid(s, escontext.as_deref_mut())? else {
        return Ok(None);
    };

    if first(s) != b'=' {
        if name.as_slice() != b"group" && name.as_slice() != b"user" {
            return ereturn_soft(
                escontext,
                PgError::error(format!("unrecognized key word: \"{}\"", lossy(&name)))
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .with_hint("ACL key word must be \"group\" or \"user\"."),
            );
        }
        let Some((s2, name2)) = getid(s, escontext.as_deref_mut())? else {
            return Ok(None);
        };
        s = s2;
        name = name2;
        if name.is_empty() {
            return ereturn_soft(
                escontext,
                PgError::error("missing name")
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .with_hint("A name must follow the \"group\" or \"user\" key word."),
            );
        }
    }

    if first(s) != b'=' {
        return ereturn_soft(
            escontext,
            PgError::error("missing \"=\" sign").with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        );
    }

    let mut privs = ACL_NO_RIGHTS;
    let mut goption = ACL_NO_RIGHTS;
    let mut read = 0u64;
    let mut i = 1usize;
    while i < s.len() && (s[i].is_ascii_alphabetic() || s[i] == b'*') {
        if s[i] == b'*' {
            goption |= read;
        } else {
            match priv_for_char(s[i]) {
                Some(p) => read = p,
                None => {
                    return ereturn_soft(
                        escontext,
                        PgError::error(format!(
                            "invalid mode character: must be one of \"{}\"",
                            core::str::from_utf8(ACL_ALL_RIGHTS_STR).unwrap()
                        ))
                        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    );
                }
            }
        }
        privs |= read;
        i += 1;
    }
    s = &s[i.min(s.len())..];

    if name.is_empty() {
        aip.ai_grantee = ACL_ID_PUBLIC;
    } else {
        aip.ai_grantee = get_role_oid(&lossy(&name), true)?;
        if aip.ai_grantee == 0 {
            return ereturn_soft(
                escontext,
                PgError::error(format!("role \"{}\" does not exist", lossy(&name)))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            );
        }
    }

    if first(s) == b'/' {
        let Some((s2, name2)) = getid(&s[1..], escontext.as_deref_mut())? else {
            return Ok(None);
        };
        s = s2;
        if name2.is_empty() {
            return ereturn_soft(
                escontext,
                PgError::error("a name must follow the \"/\" sign")
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
            );
        }
        aip.ai_grantor = get_role_oid(&lossy(&name2), true)?;
        if aip.ai_grantor == 0 {
            return ereturn_soft(
                escontext,
                PgError::error(format!("role \"{}\" does not exist", lossy(&name2)))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            );
        }
    } else {
        aip.ai_grantor = BOOTSTRAP_SUPERUSERID;
        elog::ereport(types_error::WARNING)
            .errcode(types_error::ERRCODE_INVALID_GRANTOR)
            .errmsg(format!(
                "defaulting grantor to user ID {BOOTSTRAP_SUPERUSERID}"
            ))
            .finish(err_loc())?;
    }

    aclitem_set_privs_goptions(aip, privs, goption);
    Ok(Some(s))
}

fn err_loc() -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(file!(), line!() as i32, "aclparse")
}

pub fn aclitemin(
    s: &[u8],
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Option<AclItem>> {
    let mut aip = AclItem {
        ai_grantee: 0,
        ai_grantor: 0,
        ai_privs: 0,
    };
    let Some(rest) = aclparse(s, &mut aip, escontext.as_deref_mut())? else {
        return Ok(None);
    };
    let mut i = 0usize;
    while i < rest.len() && is_space(rest[i]) {
        i += 1;
    }
    if i < rest.len() && rest[i] != 0 {
        return ereturn_soft(
            escontext,
            PgError::error("extra garbage at the end of the ACL specification")
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        );
    }
    Ok(Some(aip))
}

fn put_role_name(out: &mut Vec<u8>, roleid: u32) -> PgResult<()> {
    let Some(tuple) = SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? else {
        out.extend_from_slice(roleid.to_string().as_bytes());
        return Ok(());
    };
    let d = SysCacheGetAttrNotNull(AUTHOID, &tuple, ANUM_PG_AUTHID_ROLNAME)?;
    // SAFETY: rolname is a NUL-terminated Name (64 bytes) inside the held tuple.
    let name = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    putid(out, name.to_bytes());
    ReleaseSysCache(tuple);
    Ok(())
}

pub fn aclitemout_into(aip: &AclItem, buf: &mut Vec<u8>) -> PgResult<()> {
    if aip.ai_grantee != ACL_ID_PUBLIC {
        put_role_name(buf, aip.ai_grantee)?;
    }
    buf.push(b'=');
    for i in 0..ACL_ALL_RIGHTS_STR.len() {
        if aclitem_get_privs(aip) & (1u64 << i) != 0 {
            buf.push(ACL_ALL_RIGHTS_STR[i]);
        }
        if aclitem_get_goptions(aip) & (1u64 << i) != 0 {
            buf.push(b'*');
        }
    }
    buf.push(b'/');
    put_role_name(buf, aip.ai_grantor)?;
    Ok(())
}

pub fn aclitemout<'mcx>(mcx: Mcx<'mcx>, aip: &AclItem) -> PgResult<PgVec<'mcx, u8>> {
    let mut buf: Vec<u8> = Vec::with_capacity(2 * NAMEDATALEN + 40);
    aclitemout_into(aip, &mut buf)?;
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, buf.len() + 1)?;
    vec_append_bytes(&mut out, &buf)?;
    out.push(0);
    Ok(out)
}
