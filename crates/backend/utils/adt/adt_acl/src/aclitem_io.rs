//! `aclitem` text I/O and hashing (`utils/adt/acl.c`).
//!
//! `getid`/`putid`/`is_safe_acl_char`/`aclparse` parse and emit the
//! `grantee=privs/grantor` external form; `aclitemin`/`aclitemout` are the
//! SQL type's `_in`/`_out`; `hash_aclitem*` and `aclitem_eq`/`_match`/
//! `Comparator` support hashing and sorting.
//!
//! Routines taking the SQL soft-error context (`escontext`) return
//! `PgResult<Option<T>>`: `Ok(None)` mirrors C's `return NULL` after `ereturn`
//! saved a soft error, `Err(_)` is a hard `ereport(ERROR)`, `Ok(Some(_))` is
//! success. Role-name <-> OID resolution routes through the owner seams: the
//! sibling (same-unit) `role_membership::get_role_oid`, and the syscache
//! `authid_rolname` projection of `SearchSysCache1(AUTHOID)`.

use ::mcx::{Mcx, PgVec};
use ::types_acl::{
    aclitem_get_goptions, aclitem_get_privs, aclitem_set_privs_goptions, AclItem, AclMode,
    ACL_ALL_RIGHTS_STR, ACL_ALTER_SYSTEM, ACL_ALTER_SYSTEM_CHR, ACL_CONNECT, ACL_CONNECT_CHR,
    ACL_CREATE, ACL_CREATE_CHR, ACL_CREATE_TEMP, ACL_CREATE_TEMP_CHR, ACL_DELETE, ACL_DELETE_CHR,
    ACL_EXECUTE, ACL_EXECUTE_CHR, ACL_ID_PUBLIC, ACL_INSERT, ACL_INSERT_CHR, ACL_MAINTAIN,
    ACL_MAINTAIN_CHR, ACL_NO_RIGHTS, ACL_REFERENCES, ACL_REFERENCES_CHR, ACL_SELECT,
    ACL_SELECT_CHR, ACL_SET, ACL_SET_CHR, ACL_TRIGGER, ACL_TRIGGER_CHR, ACL_TRUNCATE,
    ACL_TRUNCATE_CHR, ACL_UPDATE, ACL_UPDATE_CHR, ACL_USAGE, ACL_USAGE_CHR, N_ACL_RIGHTS,
};
use ::types_core::{OidIsValid, BOOTSTRAP_SUPERUSERID, NAMEDATALEN};
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_GRANTOR,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NAME_TOO_LONG, ERRCODE_UNDEFINED_OBJECT,
};

use syscache_seams as syscache;

/// C `isspace` in the C locale (the only locale `acl.c` parses in).
#[inline]
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// C `isalpha` in the C locale.
#[inline]
fn is_alpha(c: u8) -> bool {
    c.is_ascii_alphabetic()
}

/// C `isalnum` in the C locale.
#[inline]
fn is_alnum(c: u8) -> bool {
    c.is_ascii_alphanumeric()
}

/// `is_safe_acl_char` (acl.c) — is `c` allowed unquoted in an ACL identifier?
/// `is_getid` distinguishes the parsing context: high-bit-set chars are
/// accepted unquoted only by `getid` (dump compatibility); `putid` always
/// quotes them.
#[inline]
pub fn is_safe_acl_char(c: u8, is_getid: bool) -> bool {
    if c & 0x80 != 0 {
        // IS_HIGHBIT_SET
        return is_getid;
    }
    is_alnum(c) || c == b'_'
}

/// `getid` (acl.c) — consume the first identifier in `s` (ignoring leading
/// whitespace; honouring double-quoting), returning it as bytes.
///
/// Operates on byte slices. Returns `Ok(Some((rest, name)))` on success where
/// `rest` is the slice position past the identifier and any trailing
/// whitespace; `Ok(None)` if a soft error was saved; `Err(_)` on a hard error.
/// `name` is the (possibly empty) parsed identifier, mirroring C's
/// `char n[NAMEDATALEN]` (bounded below `NAMEDATALEN`).
pub fn getid<'a>(
    s: &'a [u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<(&'a [u8], Vec<u8>)>> {
    let mut n: Vec<u8> = Vec::new();
    let mut in_quotes = false;

    let mut i = 0usize;
    // while (isspace((unsigned char) *s)) s++;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }

    // for ( ; *s != '\0' && (in_quotes || *s == '"' || is_safe_acl_char(*s, true)); s++)
    while i < s.len()
        && s[i] != b'\0'
        && (in_quotes || s[i] == b'"' || is_safe_acl_char(s[i], true))
    {
        if s[i] == b'"' {
            if !in_quotes {
                in_quotes = true;
                i += 1;
                continue;
            }
            // safe to look at next char (could be '\0' though)
            if i + 1 >= s.len() || s[i + 1] != b'"' {
                in_quotes = false;
                i += 1;
                continue;
            }
            // it's an escaped double quote; skip the escaping char
            i += 1;
        }

        // Add the character to the string (C: bounded by NAMEDATALEN-1).
        if n.len() >= (NAMEDATALEN as usize) - 1 {
            return ereturn(
                escontext.as_deref_mut(),
                None,
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

    // while (isspace((unsigned char) *s)) s++;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }

    Ok(Some((&s[i..], n)))
}

/// `putid` (acl.c) — append role name `s` to `p`, adding double quotes if any
/// character requires quoting (kept in sync with `dequoteAclUserName`).
pub fn putid(p: &mut Vec<u8>, s: &[u8]) {
    // Detect whether we need to use double quotes.
    let mut safe = true;
    for &c in s {
        if !is_safe_acl_char(c, false) {
            safe = false;
            break;
        }
    }
    if !safe {
        p.push(b'"');
    }
    for &c in s {
        // A double quote character in a username is encoded as ""
        if c == b'"' {
            p.push(b'"');
        }
        p.push(c);
    }
    if !safe {
        p.push(b'"');
    }
}

/// First byte of a slice, or NUL if empty (mirrors C's `*s` where `s` points
/// at a NUL terminator).
#[inline]
fn first(s: &[u8]) -> u8 {
    s.first().copied().unwrap_or(b'\0')
}

/// Lossy UTF-8 view of an identifier for error messages and role-name lookups
/// (role names are bytes in C; messages embed them verbatim).
#[inline]
fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// The non-fatal WARNING that `aclparse` emits via `ereport(WARNING)` when the
/// grantor is defaulted (it is not an error; the caller surfaces it).
pub type AclParseWarning = Option<PgError>;

/// `aclparse` (acl.c) — parse one external aclitem
/// `[group|user] name=privs[/grantor]` from `s` into `aip`.
///
/// Returns `Ok(Some(rest))` (the slice position after the spec) on success,
/// `Ok(None)` on a saved soft error, or `Err(_)` on a hard error. Any defaulted
/// grantor WARNING is recorded into `warning` for the caller to emit.
pub fn aclparse<'a>(
    s: &'a [u8],
    aip: &mut AclItem,
    mut escontext: Option<&mut SoftErrorContext>,
    warning: &mut AclParseWarning,
) -> PgResult<Option<&'a [u8]>> {
    let (mut s, mut name) = match getid(s, escontext.as_deref_mut())? {
        Some(v) => v,
        None => return Ok(None),
    };

    if first(s) != b'=' {
        // we just read a keyword, not a name
        if name.as_slice() != b"group" && name.as_slice() != b"user" {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                PgError::error(format!("unrecognized key word: \"{}\"", lossy(&name)))
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .with_hint("ACL key word must be \"group\" or \"user\"."),
            );
        }
        // move s to the name beyond the keyword
        let (s2, name2) = match getid(s, escontext.as_deref_mut())? {
            Some(v) => v,
            None => return Ok(None),
        };
        s = s2;
        name = name2;
        if name.is_empty() {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                PgError::error("missing name")
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .with_hint("A name must follow the \"group\" or \"user\" key word."),
            );
        }
    }

    if first(s) != b'=' {
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error("missing \"=\" sign")
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        );
    }

    let mut privs: AclMode = ACL_NO_RIGHTS;
    let mut goption: AclMode = ACL_NO_RIGHTS;
    let mut read: AclMode = 0;

    // for (++s, read = 0; isalpha(*s) || *s == '*'; s++)
    let mut i = 1usize;
    while i < s.len() && (is_alpha(s[i]) || s[i] == b'*') {
        let c = s[i];
        match c {
            b'*' => goption |= read,
            ACL_INSERT_CHR => read = ACL_INSERT,
            ACL_SELECT_CHR => read = ACL_SELECT,
            ACL_UPDATE_CHR => read = ACL_UPDATE,
            ACL_DELETE_CHR => read = ACL_DELETE,
            ACL_TRUNCATE_CHR => read = ACL_TRUNCATE,
            ACL_REFERENCES_CHR => read = ACL_REFERENCES,
            ACL_TRIGGER_CHR => read = ACL_TRIGGER,
            ACL_EXECUTE_CHR => read = ACL_EXECUTE,
            ACL_USAGE_CHR => read = ACL_USAGE,
            ACL_CREATE_CHR => read = ACL_CREATE,
            ACL_CREATE_TEMP_CHR => read = ACL_CREATE_TEMP,
            ACL_CONNECT_CHR => read = ACL_CONNECT,
            ACL_SET_CHR => read = ACL_SET,
            ACL_ALTER_SYSTEM_CHR => read = ACL_ALTER_SYSTEM,
            ACL_MAINTAIN_CHR => read = ACL_MAINTAIN,
            _ => {
                let s = ACL_ALL_RIGHTS_STR;
                let s = core::str::from_utf8(s).unwrap_or("");
                return ereturn(
                    escontext.as_deref_mut(),
                    None,
                    PgError::error(format!(
                        "invalid mode character: must be one of \"{s}\""
                    ))
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
                );
            }
        }
        privs |= read;
        i += 1;
    }
    s = &s[i..];

    if name.is_empty() {
        aip.ai_grantee = ACL_ID_PUBLIC;
    } else {
        // get_role_oid(name, true) — owned by this unit's role_membership family.
        aip.ai_grantee = crate::role_membership::get_role_oid(&lossy(&name), true)?;
        if !OidIsValid(aip.ai_grantee) {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                PgError::error(format!("role \"{}\" does not exist", lossy(&name)))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            );
        }
    }

    // XXX backward compatibility: default the grantor to the superuser.
    if first(s) == b'/' {
        let (s2, name2) = match getid(&s[1..], escontext.as_deref_mut())? {
            Some(v) => v,
            None => return Ok(None),
        };
        s = s2;
        if name2.is_empty() {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                PgError::error("a name must follow the \"/\" sign")
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
            );
        }
        aip.ai_grantor = crate::role_membership::get_role_oid(&lossy(&name2), true)?;
        if !OidIsValid(aip.ai_grantor) {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                PgError::error(format!("role \"{}\" does not exist", lossy(&name2)))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            );
        }
    } else {
        aip.ai_grantor = BOOTSTRAP_SUPERUSERID;
        // ereport(WARNING): non-fatal; recorded for the caller to emit.
        *warning = Some(
            PgError::warning(format!(
                "defaulting grantor to user ID {BOOTSTRAP_SUPERUSERID}"
            ))
            .with_sqlstate(ERRCODE_INVALID_GRANTOR),
        );
    }

    aclitem_set_privs_goptions(aip, privs, goption);

    Ok(Some(s))
}

/// Result of [`aclitemin`]: the parsed item plus any non-fatal WARNING that the
/// C code would have emitted via `ereport(WARNING)` (the defaulted grantor).
#[derive(Clone, Debug)]
pub struct AclItemInResult {
    pub item: AclItem,
    pub warning: AclParseWarning,
}

/// `aclitemin` (acl.c) — `aclitem` type input function: parse a complete ACL
/// specification from `s` into a new `AclItem`.
///
/// `Ok(None)` means a soft error was saved into `escontext` (C:
/// `PG_RETURN_NULL` after a soft `ereturn`). The success result carries any
/// defaulted-grantor WARNING.
pub fn aclitemin(
    s: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<AclItemInResult>> {
    let mut aip = AclItem {
        ai_grantee: 0,
        ai_grantor: 0,
        ai_privs: 0,
    };
    let mut warning: AclParseWarning = None;

    let s = match aclparse(s, &mut aip, escontext.as_deref_mut(), &mut warning)? {
        Some(rest) => rest,
        None => return Ok(None),
    };

    // while (isspace((unsigned char) *s)) ++s;  if (*s) ereturn(...)
    let mut i = 0usize;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    if i < s.len() && s[i] != b'\0' {
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error("extra garbage at the end of the ACL specification")
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        );
    }

    Ok(Some(AclItemInResult { item: aip, warning }))
}

/// `aclitemout` (acl.c) — `aclitem` type output function: format `aip` into its
/// external text representation (the raw cstring bytes, allocated in `mcx`).
///
/// The grantee/grantor role names are resolved via `SearchSysCache1(AUTHOID)`
/// (the `authid_rolname` syscache seam); a role OID with no `pg_authid` row
/// prints its numeric OID (C's `sprintf("%u")` fallback).
pub fn aclitemout<'mcx>(mcx: Mcx<'mcx>, aip: &AclItem) -> PgResult<PgVec<'mcx, u8>> {
    let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);

    if aip.ai_grantee != ACL_ID_PUBLIC {
        match syscache::authid_rolname::call(mcx, aip.ai_grantee)? {
            Some(name) => putid(&mut into_std(&mut out), name.as_bytes()),
            None => {
                // Generate numeric OID if we don't find an entry.
                out.extend_from_slice(aip.ai_grantee.to_string().as_bytes());
            }
        }
    }

    out.push(b'=');

    for i in 0..(N_ACL_RIGHTS as usize) {
        if aclitem_get_privs(*aip) & (1u64 << i) != 0 {
            out.push(ACL_ALL_RIGHTS_STR[i]);
        }
        if aclitem_get_goptions(*aip) & (1u64 << i) != 0 {
            out.push(b'*');
        }
    }

    out.push(b'/');

    match syscache::authid_rolname::call(mcx, aip.ai_grantor)? {
        Some(name) => putid(&mut into_std(&mut out), name.as_bytes()),
        None => out.extend_from_slice(aip.ai_grantor.to_string().as_bytes()),
    }

    Ok(out)
}

/// Bridge `putid` (which appends into a plain `Vec<u8>`, matching C's
/// fixed-buffer `char *p`) over a context-allocated [`PgVec`]: collect the
/// emitted bytes via a scratch `Vec` and copy them into the charged buffer.
/// (Small, transient — bounded by `2*NAMEDATALEN+2` per name in C.)
fn into_std<'mcx, 'a>(out: &'a mut PgVec<'mcx, u8>) -> PutidSink<'a, 'mcx> {
    PutidSink { out, scratch: Vec::new() }
}

/// Scratch sink so `putid` can `push` while we forward the bytes into the
/// charged [`PgVec`] on drop.
struct PutidSink<'a, 'mcx> {
    out: &'a mut PgVec<'mcx, u8>,
    scratch: Vec<u8>,
}

impl core::ops::Deref for PutidSink<'_, '_> {
    type Target = Vec<u8>;
    fn deref(&self) -> &Vec<u8> {
        &self.scratch
    }
}
impl core::ops::DerefMut for PutidSink<'_, '_> {
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.scratch
    }
}
impl Drop for PutidSink<'_, '_> {
    fn drop(&mut self) {
        self.out.extend_from_slice(&self.scratch);
    }
}

/// `aclitem_match` (acl.c) — two items match iff same grantee and grantor
/// (privileges ignored).
pub fn aclitem_match(a1: &AclItem, a2: &AclItem) -> bool {
    a1.ai_grantee == a2.ai_grantee && a1.ai_grantor == a2.ai_grantor
}

/// `aclitemComparator` (acl.c) — qsort comparator over `AclItem` (grantee, then
/// grantor, then the full rights field).
pub fn aclitem_comparator(a1: &AclItem, a2: &AclItem) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    if a1.ai_grantee > a2.ai_grantee {
        return Ordering::Greater;
    }
    if a1.ai_grantee < a2.ai_grantee {
        return Ordering::Less;
    }
    if a1.ai_grantor > a2.ai_grantor {
        return Ordering::Greater;
    }
    if a1.ai_grantor < a2.ai_grantor {
        return Ordering::Less;
    }
    if a1.ai_privs > a2.ai_privs {
        return Ordering::Greater;
    }
    if a1.ai_privs < a2.ai_privs {
        return Ordering::Less;
    }
    Ordering::Equal
}

/// `aclitem_eq` (acl.c) — SQL equality of two aclitems.
pub fn aclitem_eq(a1: &AclItem, a2: &AclItem) -> bool {
    a1.ai_privs == a2.ai_privs && a1.ai_grantee == a2.ai_grantee && a1.ai_grantor == a2.ai_grantor
}

/// `hash_aclitem` (acl.c) — 32-bit hash (avoids any issue of struct padding).
pub fn hash_aclitem(a: &AclItem) -> u32 {
    // (uint32) (a->ai_privs + a->ai_grantee + a->ai_grantor)
    (a.ai_privs as u32)
        .wrapping_add(a.ai_grantee)
        .wrapping_add(a.ai_grantor)
}

/// `hash_aclitem_extended` (acl.c) — 64-bit seeded hash. With `seed == 0` it is
/// the unseeded `hash_aclitem` sum widened to 64 bits; otherwise it runs the
/// `hash_bytes_uint32_extended` mixer from `common/hashfn.c`.
pub fn hash_aclitem_extended(a: &AclItem, seed: u64) -> u64 {
    let sum = (a.ai_privs as u32)
        .wrapping_add(a.ai_grantee)
        .wrapping_add(a.ai_grantor);
    if seed == 0 {
        sum as u64
    } else {
        hash_uint32_extended(sum, seed)
    }
}

/// `hash_bytes_uint32_extended(k, seed)` (`common/hashfn.c`) — the seeded
/// 32-bit -> 64-bit hash used by [`hash_aclitem_extended`]. Faithful port,
/// including the `0x9e3779b9 + sizeof(uint32) + 3923095` initializer.
fn hash_uint32_extended(k: u32, seed: u64) -> u64 {
    // a = b = c = 0x9e3779b9 + sizeof(uint32) + 3923095
    let init: u32 = 0x9e37_79b9u32.wrapping_add(4).wrapping_add(3923095);
    let mut a = init;
    let mut b = init;
    let mut c = init;

    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        let (na, nb, nc) = mix(a, b, c);
        a = na;
        b = nb;
        c = nc;
    }

    a = a.wrapping_add(k);

    let (_a, b, c) = final_mix(a, b, c);

    ((b as u64) << 32) | (c as u64)
}

#[inline]
fn rot(x: u32, k: u32) -> u32 {
    x.rotate_left(k)
}

#[inline]
fn mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    a = a.wrapping_sub(c);
    a ^= rot(c, 4);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= rot(a, 6);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= rot(b, 8);
    b = b.wrapping_add(a);
    a = a.wrapping_sub(c);
    a ^= rot(c, 16);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= rot(a, 19);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= rot(b, 4);
    b = b.wrapping_add(a);
    (a, b, c)
}

#[inline]
fn final_mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    c ^= b;
    c = c.wrapping_sub(rot(b, 14));
    a ^= c;
    a = a.wrapping_sub(rot(c, 11));
    b ^= a;
    b = b.wrapping_sub(rot(a, 25));
    c ^= b;
    c = c.wrapping_sub(rot(b, 16));
    a ^= c;
    a = a.wrapping_sub(rot(c, 4));
    b ^= a;
    b = b.wrapping_sub(rot(a, 14));
    c ^= b;
    c = c.wrapping_sub(rot(b, 24));
    (a, b, c)
}
