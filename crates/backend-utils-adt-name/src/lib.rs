#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! Port of PostgreSQL 18.3 `src/backend/utils/adt/name.c`: the built-in type
//! `name` — a fixed-length, `NAMEDATALEN`-byte, NUL-padded identifier string
//! (the on-disk catalog identifier representation).
//!
//! `name` is pass-by-reference; its referent is a fixed-size [`NameData`]
//! ([`types_tuple::heaptuple::NameData`]). Following the sibling adt ports
//! (`backend-utils-adt-char`), these are plain typed Rust functions, not an
//! fmgr/`Datum` marshalling layer: a `name` value crosses as `&NameData`, a
//! `cstring` as `&str`, binary I/O uses [`StringInfo`].
//!
//! Calls into other units go through the owner's `-seams` crate when a direct
//! dependency would cycle: `pg_mbcliplen` (mbutils), `fetch_search_path`
//! (namespace), `get_namespace_name` (lsyscache), `GetUserId` /
//! `GetSessionUserId` / `GetUserNameFromId` (miscinit), and `build_name_array`
//! (arrayfuncs). The non-cyclic `varstr_cmp` / `varstr_sortsupport` (varlena)
//! and the `pqformat` helpers are called directly.

extern crate alloc;

pub mod fmgr_builtins;

use alloc::format;

use mcx::{Mcx, PgString, PgVec};
use types_core::{Oid, C_COLLATION_OID, NAMEDATALEN};
use types_datum::Bytea;
use types_error::{PgResult, ERRCODE_NAME_TOO_LONG};
use types_sortsupport::SortSupportData;
use types_stringinfo::StringInfo;
use types_tuple::heaptuple::{NameData, NAMEOID};

use backend_libpq_pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgtext, pq_sendtext};
use backend_utils_adt_varlena::comparison::varstr_cmp;
use backend_utils_adt_varlena::sortsupport::{varstr_sortsupport, VarStrSortSupport};
use types_error::PgError;

use backend_catalog_namespace_seams as namespace_seam;
use backend_utils_adt_arrayfuncs_seams as arrayfuncs_seam;
use backend_utils_cache_lsyscache_seams as lsyscache_seam;
use backend_utils_init_miscinit_seams as miscinit_seam;
use backend_utils_mb_mbutils_seams as mbutils_seam;

// ===========================================================================
// USER I/O ROUTINES
// ===========================================================================

/// `namein` (name.c:48): converts a `cstring` to the internal `name`
/// representation.
///
/// Oversize input (`len >= NAMEDATALEN`) is clipped to `NAMEDATALEN - 1` bytes
/// on an encoded-character boundary via `pg_mbcliplen`. The result is always a
/// zero-padded `NAMEDATALEN`-byte block, so the name is NUL-terminated.
pub fn namein(s: &str) -> PgResult<NameData> {
    let bytes = s.as_bytes();
    let mut len = bytes.len() as i32;

    // Truncate oversize input.
    if len >= NAMEDATALEN {
        len = mbutils_seam::pg_mbcliplen::call(bytes, len, NAMEDATALEN - 1);
    }

    // palloc0 + memcpy: a zero-padded NAMEDATALEN block.
    let mut result = NameData::default();
    let len = len as usize;
    result.data[..len].copy_from_slice(&bytes[..len]);
    Ok(result)
}

/// `nameout` (name.c:67): converts a `name` to a `cstring` via
/// `pstrdup(NameStr(*s))` — the name's bytes up to the first NUL.
pub fn nameout<'mcx>(mcx: Mcx<'mcx>, s: &NameData) -> PgResult<PgString<'mcx>> {
    let mut result = PgString::new_in(mcx);
    // NameStr bytes are server-encoding catalog identifiers; surface them as
    // UTF-8 with a lossy fallback (matches the byte content for ASCII names).
    let text = alloc::string::String::from_utf8_lossy(s.name_str());
    result.try_push_str(&text)?;
    Ok(result)
}

/// `namerecv` (name.c:77): converts external binary format to a `name`.
pub fn namerecv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'mcx>) -> PgResult<NameData> {
    // str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    let rawbytes = buf.data.len() - buf.cursor;
    let str = pq_getmsgtext(mcx, buf, rawbytes)?;
    let nbytes = str.len();
    if nbytes >= NAMEDATALEN as usize {
        // ereport(ERROR, errcode(ERRCODE_NAME_TOO_LONG), errmsg(...), errdetail(...))
        return Err(PgError::error("identifier too long")
            .with_sqlstate(ERRCODE_NAME_TOO_LONG)
            .with_detail(format!(
                "Identifier must be less than {NAMEDATALEN} characters."
            )));
    }
    let mut result = NameData::default();
    result.data[..nbytes].copy_from_slice(&str);
    Ok(result)
}

/// `namesend` (name.c:101): converts a `name` to binary format.
pub fn namesend<'mcx>(mcx: Mcx<'mcx>, s: &NameData) -> PgResult<Bytea<'mcx>> {
    let mut buf = pq_begintypsend(mcx)?;
    pq_sendtext(&mut buf, s.name_str())?;
    Ok(pq_endtypsend(buf))
}

// ===========================================================================
// COMPARISON/SORTING ROUTINES
// ===========================================================================

/// `strncmp(a, b, n)` over `unsigned char` (C string semantics): stop at the
/// first difference, a NUL, or `n` bytes. A `NameData` block is always
/// NUL-padded, so this matches C reading `NameStr(*arg)` to `NAMEDATALEN`.
fn strncmp(a: &[u8], b: &[u8], n: usize) -> i32 {
    for i in 0..n {
        let ca = a.get(i).copied().unwrap_or(0);
        let cb = b.get(i).copied().unwrap_or(0);
        if ca != cb {
            return if ca < cb { -1 } else { 1 };
        }
        if ca == 0 {
            return 0;
        }
    }
    0
}

/// `namecmp` (name.c:135) — the shared comparison worker for the `name`
/// comparison operators.
///
/// Fast path for `C_COLLATION_OID` (the common system-catalog case) does a raw
/// `strncmp(NameStr(*arg1), NameStr(*arg2), NAMEDATALEN)`; any other collation
/// goes through `varstr_cmp` (varlena.c).
fn namecmp(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<i32> {
    if collid == C_COLLATION_OID {
        return Ok(strncmp(&arg1.data, &arg2.data, NAMEDATALEN as usize));
    }
    varstr_cmp(arg1.name_str(), arg2.name_str(), collid)
}

/// `nameeq` (name.c:155).
pub fn nameeq(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? == 0)
}

/// `namene` (name.c:164).
pub fn namene(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? != 0)
}

/// `namelt` (name.c:173).
pub fn namelt(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? < 0)
}

/// `namele` (name.c:182).
pub fn namele(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? <= 0)
}

/// `namegt` (name.c:191).
pub fn namegt(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? > 0)
}

/// `namege` (name.c:200).
pub fn namege(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? >= 0)
}

/// `btnamecmp` (name.c:209).
pub fn btnamecmp(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<i32> {
    namecmp(arg1, arg2, collid)
}

/// `btnamesortsupport` (name.c:218): install the generic string SortSupport
/// for `name` under the function's collation. C switches into `ssup->ssup_cxt`
/// before allocating; here `varstr_sortsupport` charges its scratch to
/// `ssup.ssup_cxt`. The decision struct is returned for the caller's
/// comparator dispatch (see [`VarStrSortSupport`]).
pub fn btnamesortsupport<'mcx>(
    ssup: &mut SortSupportData<'mcx>,
) -> PgResult<VarStrSortSupport<'mcx>> {
    let collid = ssup.ssup_collation;
    varstr_sortsupport(ssup, NAMEOID, collid)
}

// ===========================================================================
// MISCELLANEOUS PUBLIC ROUTINES
// ===========================================================================

/// `namestrcpy` (name.c:241): copy a C string into a fixed-size `Name`,
/// NUL-terminating and zero-padding. The source is truncated to
/// `NAMEDATALEN - 1` bytes.
pub fn namestrcpy(name: &mut NameData, str: &str) {
    name.namestrcpy(str);
}

/// `namestrcmp` (name.c:253): compare a `NAME` to a C string, assuming the C
/// collation. NULL handling: both NULL → 0; a NULL `name` → -1; a NULL `str`
/// → 1 (C's "NULL < anything", verbatim).
pub fn namestrcmp(name: Option<&NameData>, str: Option<&str>) -> i32 {
    match (name, str) {
        (None, None) => 0,
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (Some(name), Some(str)) => {
            strncmp(&name.data, str.as_bytes(), NAMEDATALEN as usize)
        }
    }
}

// ===========================================================================
// SQL-functions CURRENT_USER, SESSION_USER
// ===========================================================================

/// `current_user` (name.c:275): `namein(GetUserNameFromId(GetUserId(), false))`.
pub fn current_user(mcx: Mcx<'_>) -> PgResult<NameData> {
    let userid = miscinit_seam::get_user_id::call();
    let name = miscinit_seam::get_user_name_from_id::call(mcx, userid, false)?
        .expect("GetUserNameFromId(noerr=false) returns a name or raises ERROR");
    namein(name.as_str())
}

/// `session_user` (name.c:281):
/// `namein(GetUserNameFromId(GetSessionUserId(), false))`.
pub fn session_user(mcx: Mcx<'_>) -> PgResult<NameData> {
    let userid = miscinit_seam::get_session_user_id::call();
    let name = miscinit_seam::get_user_name_from_id::call(mcx, userid, false)?
        .expect("GetUserNameFromId(noerr=false) returns a name or raises ERROR");
    namein(name.as_str())
}

// ===========================================================================
// SQL-functions CURRENT_SCHEMA, CURRENT_SCHEMAS
// ===========================================================================

/// `current_schema` (name.c:291): the first namespace on the active search
/// path, as a `name`. Returns `Ok(None)` (SQL NULL) when the path is empty or
/// the first namespace was recently deleted.
pub fn current_schema(mcx: Mcx<'_>) -> PgResult<Option<NameData>> {
    let search_path = namespace_seam::fetch_search_path::call(mcx, false)?;
    if search_path.is_empty() {
        return Ok(None); // NIL
    }
    let nspname = lsyscache_seam::get_namespace_name::call(mcx, search_path[0])?;
    match nspname {
        None => Ok(None), // recently-deleted namespace?
        Some(nspname) => Ok(Some(namein(nspname.as_str())?)),
    }
}

/// `current_schemas(include_implicit)` (name.c:307): the active search path as
/// a `name[]` array, skipping recently-deleted namespaces. The result is the
/// array varlena's raw bytes (the canonical by-reference payload).
pub fn current_schemas<'mcx>(
    mcx: Mcx<'mcx>,
    include_implicit: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let search_path = namespace_seam::fetch_search_path::call(mcx, include_implicit)?;

    // names = palloc(list_length * sizeof(Datum)); for each oid with a live
    // namespace name, names[i] = namein(nspname). Build the NameData images.
    let mut names: PgVec<'mcx, NameData> =
        mcx::vec_with_capacity_in(mcx, search_path.len())?;
    for &oid in search_path.iter() {
        let nspname = lsyscache_seam::get_namespace_name::call(mcx, oid)?;
        if let Some(nspname) = nspname {
            // watch out for deleted namespace
            names.push(namein(nspname.as_str())?);
        }
    }

    // array = construct_array_builtin(names, i, NAMEOID);
    let images: PgVec<'mcx, &[u8]> = {
        let mut v = mcx::vec_with_capacity_in(mcx, names.len())?;
        for n in names.iter() {
            v.push(&n.data[..]);
        }
        v
    };
    arrayfuncs_seam::build_name_array::call(mcx, images.as_slice())
}

// ===========================================================================
// SQL-function nameconcatoid
// ===========================================================================

/// `nameconcatoid(name, oid)` (name.c:340): append `'_' || oid` to the name,
/// truncating the *name* part (not the oid suffix) if the result would not fit
/// in `NAMEDATALEN`. Used by `information_schema` to make per-schema-unique
/// `specific_name` columns.
pub fn nameconcatoid(nam: &NameData, oid: Oid) -> PgResult<NameData> {
    // suflen = snprintf(suffix, sizeof(suffix), "_%u", oid);
    let suffix = format!("_{oid}");
    let suffix = suffix.as_bytes();
    let suflen = suffix.len() as i32;

    let name_str = nam.name_str();
    let mut namlen = name_str.len() as i32;

    // Truncate oversize input by truncating name part, not suffix.
    if namlen + suflen >= NAMEDATALEN {
        namlen = mbutils_seam::pg_mbcliplen::call(name_str, namlen, NAMEDATALEN - 1 - suflen);
    }

    // palloc0 + memcpy(name) + memcpy(suffix).
    let mut result = NameData::default();
    let namlen = namlen as usize;
    let suflen = suflen as usize;
    result.data[..namlen].copy_from_slice(&name_str[..namlen]);
    result.data[namlen..namlen + suflen].copy_from_slice(&suffix[..suflen]);
    Ok(result)
}

/// This unit owns no inward `-seams` crate (its value cores are consumed
/// directly). `init_seams()` registers the `name.c` fmgr builtins into the
/// fmgr-core builtin table so `fmgr_isbuiltin` resolves them on the fast path
/// (catalog name-column scankeys need `nameeq` before any catalog access).
pub fn init_seams() {
    fmgr_builtins::register_name_builtins();
}

#[cfg(test)]
mod tests;
