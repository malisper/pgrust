#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the workspace-wide `PgResult` (== `Result<_,
// PgError>`); `PgError`'s size is fixed by `types-error` and boxing it locally
// would diverge from every sibling crate's signatures. Accept the large-`Err`
// lint crate-wide.
#![allow(clippy::result_large_err)]

//! Port of `src/backend/utils/adt/varchar.c`: the `bpchar` (blank-padded
//! `char(n)`) and `varchar`/`varchar(n)` datatypes — I/O (`*in`/`*out`/`*recv`/
//! `*send`), typmod in/out, the `bpchar()`/`varchar()` length-coercion casts,
//! the `char`/`name`/`bpchar` conversions, the exported length helpers, and the
//! collation-aware comparison / hashing / pattern-ordering operators.
//!
//! # Carrier model (repo convention)
//!
//! `bpchar`/`varchar` are binary-compatible with `text`; throughout this repo a
//! `text`/`bpchar`/`varchar` *value* is carried as its header-less payload bytes
//! (`VARDATA`), with the 4-byte varlena length word living only at the
//! Datum/FFI boundary (mirrors `backend-utils-adt-varlena`'s
//! `cstring_to_text_with_len`, which returns the payload `PgVec<'mcx, u8>`). So:
//!
//! * functions that *produce* a `bpchar`/`varchar` value return the owned
//!   payload bytes as a [`mcx::PgVec<'mcx, u8>`] charged to the caller's
//!   [`mcx::Mcx`] (C: `palloc` in `CurrentMemoryContext` + `SET_VARSIZE` —
//!   the header is re-stamped at the boundary);
//! * comparison / hash / length functions take `&[u8]` = the already-detoasted
//!   `VARDATA` payload (the caller's fmgr glue does the detoast).
//!
//! `*send` returns a full [`types_datum::Bytea`] image (header + payload), built
//! by the shared `textsend` path in `backend-utils-adt-varlena`.
//!
//! # The fmgr / `Datum` boundary
//!
//! The SQL-callable wrappers in C take `PG_FUNCTION_ARGS` / return `Datum`; per
//! the project-wide deferral that boundary is modeled by giving each function
//! the already-unwrapped arguments (`&[u8]` VARDATA, `i32` typmod, `bool` flags,
//! `Oid` collation) and an owned/typed return. No `*mut`/`extern "C"`/`Datum`
//! appears here. The bare-word PGFunction registry is deferred project-wide.
//!
//! # Dependencies (real owners, reached directly or via seams)
//!
//! * `backend-utils-adt-varlena` (landed) — `cstring_to_text_with_len`,
//!   `check_collation_set`, `varstr_cmp`, `textsend`, `bpchartruelen`;
//! * `backend-utils-mb-fgram` / `backend-utils-mb-mbutils-seams` — multibyte
//!   helpers `pg_mbstrlen_with_len`, `pg_mbcharcliplen`, `pg_mbcliplen`,
//!   `pg_database_encoding_max_length`;
//! * `backend-utils-adt-pg-locale-seams` — `collation_is_deterministic`,
//!   `pg_strxfrm` (the `pg_strnxfrm` analog);
//! * `backend-utils-adt-arrayutils` — `array_get_integer_typmods`;
//! * `common-hashfn` — `hash_bytes` / `hash_bytes_extended` (PG `hash_any` /
//!   `hash_any_extended`);
//! * `backend-libpq-pqformat` — `pq_getmsgtext` for `*recv`.
//!
//! `varchar_support` (planner node simplification, `supportnodes.h`) and
//! `*_sortsupport` (the C-ABI comparator install into `SortSupportData`) reach
//! still-evolving owners through the existing real seams/pub fns; see those
//! functions.

extern crate alloc;

use alloc::format;

pub mod fmgr_builtins;

/// Install this unit's inward seams and register its `varchar.c` fmgr builtins
/// (so `fmgr_isbuiltin` resolves them on the by-OID fast path). Called by
/// `seams-init::init_all`.
pub fn init_seams() {
    fmgr_builtins::register_varchar_builtins();
}

use backend_utils_adt_varlena::comparison::varstr_cmp;
use backend_utils_adt_varlena::keystone::{check_collation_set, cstring_to_text_with_len};
use backend_utils_adt_varlena::sortsupport::{bpchartruelen, varstr_sortsupport};
use backend_utils_adt_varlena::wire_io::textsend;
use common_hashfn::{hash_bytes, hash_bytes_extended};
use mcx::{Mcx, PgVec};
use types_datum::Bytea;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INDETERMINATE_COLLATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_STRING_DATA_RIGHT_TRUNCATION,
};
use types_sortsupport::SortSupportData;
use types_stringinfo::StringInfo;

use backend_utils_adt_pg_locale_seams as loc;
use backend_utils_mb_mbutils_seams as mb;

/// `MaxAttrSize` (htup_details.h): 10 * 1024 * 1024.
const MAX_ATTR_SIZE: i32 = 10 * 1024 * 1024;

/// `BPCHAROID` (pg_type.h).
pub const BPCHAROID: types_core::Oid = 1042;

/// `C_COLLATION_OID` (pg_collation.h).
pub const C_COLLATION_OID: types_core::Oid = 950;

/// `NAMEDATALEN` (pg_config_manual.h): the fixed `NameData` width, in bytes.
pub const NAMEDATALEN: i32 = 64;

/// `VARHDRSZ` — the uncompressed varlena length-word size, in bytes.
pub const VARHDRSZ: i32 = 4;

type Oid = types_core::Oid;
const InvalidOid: Oid = 0;

// ===========================================================================
// internal typmod helpers (shared by bpchar/varchar)  -- varchar.c:32,71
// ===========================================================================

/// `anychar_typmodin` — shared `typmodin` validator (varchar.c:32). The
/// `ArrayGetIntegerTypmods` decode is done by the caller; `tl` is the decoded
/// modifier list.
fn anychar_typmodin(tl: &[i32], typename: &str) -> PgResult<i32> {
    // we're not too tense about good error message here because grammar
    // shouldn't allow wrong number of modifiers for CHAR
    if tl.len() != 1 {
        return Err(
            PgError::error("invalid type modifier").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        );
    }

    let tl0 = tl[0];

    if tl0 < 1 {
        return Err(
            PgError::error(format!("length for type {typename} must be at least 1"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }
    if tl0 > MAX_ATTR_SIZE {
        return Err(PgError::error(format!(
            "length for type {typename} cannot exceed {MAX_ATTR_SIZE}"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // For largely historical reasons, the typmod is VARHDRSZ plus the number
    // of characters; there is enough client-side code that knows about that
    // that we'd better not change it.
    Ok(VARHDRSZ + tl0)
}

/// `anychar_typmodout` — shared `typmodout` formatter (varchar.c:71). C
/// `palloc`s a 64-byte buffer and `snprintf`s `"(%d)"` (or the empty string).
/// The owned surface returns the NUL-terminated cstring bytes charged to `mcx`.
fn anychar_typmodout<'mcx>(mcx: Mcx<'mcx>, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    if typmod > VARHDRSZ {
        let text = format!("({})", typmod - VARHDRSZ);
        cstring(mcx, text.as_bytes())
    } else {
        // *res = '\0' — empty cstring.
        cstring(mcx, b"")
    }
}

// ---------------------------------------------------------------------------
// bpchar - char(n)
// ---------------------------------------------------------------------------

/// `bpchar_input` — common guts of `bpcharin` and `bpcharrecv` (varchar.c:129).
///
/// `s` is the input text payload (may not be NUL-terminated); `atttypmod` is the
/// typmod to apply (measured in *characters*). Too-long input is an error unless
/// the extra characters are spaces, in which case they are truncated (per SQL).
/// Soft errors route through `escontext`; on a soft error `Ok(None)` is returned.
/// The returned value is the header-less `bpchar` payload (blank-padded).
fn bpchar_input<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    atttypmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let mut len = s.len();
    let maxlen: usize;

    // If typmod is -1 (or invalid), use the actual string length
    if atttypmod < VARHDRSZ {
        maxlen = len;
    } else {
        // number of CHARACTERS allowed for this bpchar type
        let maxchars = (atttypmod - VARHDRSZ) as usize;
        let charlen = mb::pg_mbstrlen_with_len::call(s, s.len() as i32)? as usize;
        if charlen > maxchars {
            // Verify that extra characters are spaces, and clip them off
            let mbmaxlen = mb::pg_mbcharcliplen::call(s, len as i32, maxchars as i32)? as usize;

            // at this point, len is the actual BYTE length of the input string,
            // maxchars is the max number of CHARACTERS, mbmaxlen is the length
            // in BYTES of those chars.
            for &b in &s[mbmaxlen..len] {
                if b != b' ' {
                    return ereturn(
                        escontext.as_deref_mut(),
                        None,
                        value_too_long_character(maxchars as i32),
                    );
                }
            }

            // Now we set maxlen to the necessary byte length, not the number of
            // CHARACTERS!
            len = mbmaxlen;
            maxlen = mbmaxlen;
        } else {
            // Now we set maxlen to the necessary byte length, not the number of
            // CHARACTERS!
            maxlen = len + (maxchars - charlen);
        }
    }

    // C: result = palloc(maxlen + VARHDRSZ); SET_VARSIZE; memcpy(r, s, len);
    // blank-pad. Carrier is the header-less payload, so build exactly `maxlen`
    // payload bytes: the first `len` are the input, the rest are blanks.
    let mut result = mcx::vec_with_capacity_in(mcx, maxlen)?;
    result.extend_from_slice(&s[..len]);
    if maxlen > len {
        result.resize(maxlen, b' ');
    }

    Ok(Some(result))
}

/// `bpcharin` — convert a C string to CHARACTER internal representation
/// (varchar.c:197). `s` is the input cstring payload (no trailing NUL);
/// `atttypmod` is the declared length plus VARHDRSZ.
pub fn bpcharin<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    _typelem: Oid,
    atttypmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    bpchar_input(mcx, s, atttypmod, escontext)
}

/// `bpcharout` — convert a CHARACTER value to a C string (varchar.c:218). Uses
/// the text conversion functions (BpChar and text are equivalent), so this is
/// `TextDatumGetCString`: the payload bytes followed by a NUL.
pub fn bpcharout<'mcx>(mcx: Mcx<'mcx>, source: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    cstring(mcx, source)
}

/// `bpcharrecv` — external binary format to bpchar (varchar.c:229).
pub fn bpcharrecv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    _typelem: Oid,
    atttypmod: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    let rawbytes = buf.data.len().saturating_sub(buf.cursor);
    let str = backend_libpq_pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    // pq_getmsgtext appends a trailing NUL (cstring contract); bpchar_input
    // operates on the `nbytes` payload, not the NUL.
    let payload = &str[..str.len().saturating_sub(1)];
    // result = bpchar_input(str, nbytes, atttypmod, NULL); -- hard-error context
    let result = bpchar_input(mcx, payload, atttypmod, None)?;
    result.ok_or_else(|| PgError::error("bpcharrecv: bpchar_input with no escontext returned None"))
}

/// `bpcharsend` — convert bpchar to binary format (varchar.c:250). Exactly the
/// same as `textsend`, so share code.
pub fn bpcharsend<'mcx>(mcx: Mcx<'mcx>, source: &[u8]) -> PgResult<Bytea<'mcx>> {
    textsend(mcx, source)
}

/// Result of [`bpchar`] / [`varchar`]: either the caller's source value is
/// returned unchanged (`PG_RETURN_BPCHAR_P(source)`), or a freshly-built value.
/// Mirrors the C "no work needed" fast paths exactly.
#[derive(Debug)]
pub enum CoerceResult<'mcx> {
    /// Return the input value unchanged.
    Source,
    /// A new value was built (the header-less payload bytes).
    New(PgVec<'mcx, u8>),
}

/// `bpchar` — coerce a CHARACTER value to the specified size (varchar.c:270).
///
/// `maxlen` is the typmod (declared length + VARHDRSZ); `is_explicit` is true for
/// an explicit cast (silent truncation) vs implicit (error unless extra chars
/// are spaces). Returns the source unchanged when no work is needed. `source` is
/// the detoasted VARDATA payload.
pub fn bpchar<'mcx>(
    mcx: Mcx<'mcx>,
    source: &[u8],
    maxlen: i32,
    is_explicit: bool,
) -> PgResult<CoerceResult<'mcx>> {
    // No work if typmod is invalid
    if maxlen < VARHDRSZ {
        return Ok(CoerceResult::Source);
    }

    let mut maxlen = maxlen - VARHDRSZ;

    let mut len = source.len() as i32;
    let s = source;

    let charlen = mb::pg_mbstrlen_with_len::call(s, s.len() as i32)?;

    // No work if supplied data matches typmod already
    if charlen == maxlen {
        return Ok(CoerceResult::Source);
    }

    if charlen > maxlen {
        // Verify that extra characters are spaces, and clip them off
        let maxmblen = mb::pg_mbcharcliplen::call(s, len, maxlen)?;

        if !is_explicit {
            for &b in &s[maxmblen as usize..len as usize] {
                if b != b' ' {
                    return Err(value_too_long_character(maxlen));
                }
            }
        }

        len = maxmblen;

        // At this point, maxlen is the necessary byte length, not the number of
        // CHARACTERS!
        maxlen = len;
    } else {
        // At this point, maxlen is the necessary byte length, not the number of
        // CHARACTERS!
        maxlen = len + (maxlen - charlen);
    }

    debug_assert!(maxlen >= len);

    // C: result = palloc(maxlen + VARHDRSZ); memcpy(r, s, len); blank-pad.
    let mut result = mcx::vec_with_capacity_in(mcx, maxlen as usize)?;
    result.extend_from_slice(&s[..len as usize]);
    if maxlen > len {
        result.resize(maxlen as usize, b' ');
    }

    Ok(CoerceResult::New(result))
}

/// `char_bpchar` — convert a `"char"` to `bpchar(1)` (varchar.c:352).
pub fn char_bpchar<'mcx>(mcx: Mcx<'mcx>, c: i8) -> PgResult<PgVec<'mcx, u8>> {
    // C: result = palloc(VARHDRSZ + 1); SET_VARSIZE(result, VARHDRSZ + 1);
    // *VARDATA(result) = c;  -- one payload byte.
    let mut result = mcx::vec_with_capacity_in(mcx, 1)?;
    result.push(c as u8);
    Ok(result)
}

/// `bpchar_name` — convert a `bpchar()` to a `NameData` (varchar.c:370).
/// `source` is the detoasted VARDATA payload. Returns NAMEDATALEN zero-padded
/// bytes (the `NameData`).
pub fn bpchar_name<'mcx>(mcx: Mcx<'mcx>, source: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let s_data = source;
    let mut len = source.len() as i32;

    // Truncate oversize input
    if len >= NAMEDATALEN {
        len = mb::pg_mbcliplen::call(s_data, len, NAMEDATALEN - 1);
    }

    // Remove trailing blanks
    while len > 0 {
        if s_data[(len - 1) as usize] != b' ' {
            break;
        }
        len -= 1;
    }

    // C: result = (Name) palloc0(NAMEDATALEN); memcpy(NameStr(*result), s_data, len);
    // -- zero-padded fixed NAMEDATALEN buffer.
    let mut result = mcx::vec_with_capacity_in(mcx, NAMEDATALEN as usize)?;
    result.extend_from_slice(&s_data[..len as usize]);
    result.resize(NAMEDATALEN as usize, 0);
    Ok(result)
}

/// `name_bpchar` — convert a `NameData` to a `bpchar` (varchar.c:406). Uses the
/// text conversion functions; `cstring_to_text(NameStr(*s))` stops at the first
/// NUL of the fixed-size name. `name` is the raw NAMEDATALEN bytes.
pub fn name_bpchar<'mcx>(mcx: Mcx<'mcx>, name: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // NameStr(*s) is a C string: take bytes up to the first NUL.
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    cstring_to_text_with_len(mcx, name, end as i32)
}

/// `bpchartypmodin` (varchar.c:416). `ta` is the `cstring[]` typmod array's
/// payload bytes; decoded via `ArrayGetIntegerTypmods`.
pub fn bpchartypmodin<'mcx>(mcx: Mcx<'mcx>, ta: &[u8]) -> PgResult<i32> {
    let tl = backend_utils_adt_arrayutils::array_get_integer_typmods(mcx, ta)?;
    anychar_typmodin(&tl, "char")
}

/// `bpchartypmodout` (varchar.c:424).
pub fn bpchartypmodout<'mcx>(mcx: Mcx<'mcx>, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    anychar_typmodout(mcx, typmod)
}

/// [`bpchartypmodin`] past the `cstring[]` decode: the `ArrayGetIntegerTypmods`
/// result is supplied directly (the parser's `typmodin` seam already carries the
/// decoded modifier list). Same validator, same `"char"` type-name
/// (varchar.c:416 + 32).
pub fn bpchartypmodin_typmods(tl: &[i32]) -> PgResult<i32> {
    anychar_typmodin(tl, "char")
}

/// [`varchartypmodin`] past the `cstring[]` decode (varchar.c:647 + 32); see
/// [`bpchartypmodin_typmods`].
pub fn varchartypmodin_typmods(tl: &[i32]) -> PgResult<i32> {
    anychar_typmodin(tl, "varchar")
}

// ---------------------------------------------------------------------------
// varchar - varchar(n)
// ---------------------------------------------------------------------------

/// `varchar_input` — common guts of `varcharin` and `varcharrecv`
/// (varchar.c:456). `atttypmod` is measured in characters. Too-long input is an
/// error unless the extra characters are spaces (truncated). Soft errors route
/// through `escontext`. Returns the header-less `varchar` payload.
fn varchar_input<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    atttypmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let mut len = s.len();
    // size_t maxlen = atttypmod - VARHDRSZ;  (computed unconditionally in C)
    let maxlen = (atttypmod - VARHDRSZ) as usize;

    if atttypmod >= VARHDRSZ && len > maxlen {
        // Verify that extra characters are spaces, and clip them off
        let mbmaxlen = mb::pg_mbcharcliplen::call(s, len as i32, maxlen as i32)? as usize;

        for &b in &s[mbmaxlen..len] {
            if b != b' ' {
                return ereturn(
                    escontext.as_deref_mut(),
                    None,
                    value_too_long_varying(maxlen as i32),
                );
            }
        }

        len = mbmaxlen;
    }

    // VarChar and text are binary-compatible types: cstring_to_text_with_len.
    let result = cstring_to_text_with_len(mcx, &s[..len], len as i32)?;
    Ok(Some(result))
}

/// `varcharin` — convert a C string to VARCHAR internal representation
/// (varchar.c:495). `atttypmod` is the declared length plus VARHDRSZ.
pub fn varcharin<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    _typelem: Oid,
    atttypmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    varchar_input(mcx, s, atttypmod, escontext)
}

/// `varcharout` — convert a VARCHAR value to a C string (varchar.c:516).
pub fn varcharout<'mcx>(mcx: Mcx<'mcx>, source: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    cstring(mcx, source)
}

/// `varcharrecv` — external binary format to varchar (varchar.c:527).
pub fn varcharrecv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    _typelem: Oid,
    atttypmod: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let rawbytes = buf.data.len().saturating_sub(buf.cursor);
    let str = backend_libpq_pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    let payload = &str[..str.len().saturating_sub(1)];
    let result = varchar_input(mcx, payload, atttypmod, None)?;
    result
        .ok_or_else(|| PgError::error("varcharrecv: varchar_input with no escontext returned None"))
}

/// `varcharsend` — convert varchar to binary format (varchar.c:548). Exactly the
/// same as `textsend`.
pub fn varcharsend<'mcx>(mcx: Mcx<'mcx>, source: &[u8]) -> PgResult<Bytea<'mcx>> {
    textsend(mcx, source)
}

/// `varchar` — coerce a VARCHAR value to the specified size (varchar.c:608).
///
/// `typmod` is the declared length + VARHDRSZ; `is_explicit` selects silent
/// truncation (explicit cast) vs error-unless-spaces (implicit). `source` is the
/// detoasted VARDATA payload.
pub fn varchar<'mcx>(
    mcx: Mcx<'mcx>,
    source: &[u8],
    typmod: i32,
    is_explicit: bool,
) -> PgResult<CoerceResult<'mcx>> {
    let len = source.len() as i32;
    let s_data = source;
    let maxlen = typmod - VARHDRSZ;

    // No work if typmod is invalid or supplied data fits it already
    if maxlen < 0 || len <= maxlen {
        return Ok(CoerceResult::Source);
    }

    // only reach here if string is too long...

    // truncate multibyte string preserving multibyte boundary
    let maxmblen = mb::pg_mbcharcliplen::call(s_data, len, maxlen)?;

    if !is_explicit {
        for &b in &s_data[maxmblen as usize..len as usize] {
            if b != b' ' {
                return Err(value_too_long_varying(maxlen));
            }
        }
    }

    let result = cstring_to_text_with_len(mcx, &s_data[..maxmblen as usize], maxmblen)?;
    Ok(CoerceResult::New(result))
}

/// `varchartypmodin` (varchar.c:647).
pub fn varchartypmodin<'mcx>(mcx: Mcx<'mcx>, ta: &[u8]) -> PgResult<i32> {
    let tl = backend_utils_adt_arrayutils::array_get_integer_typmods(mcx, ta)?;
    anychar_typmodin(&tl, "varchar")
}

/// `varchartypmodout` (varchar.c:655).
pub fn varchartypmodout<'mcx>(mcx: Mcx<'mcx>, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    anychar_typmodout(mcx, typmod)
}

// ---------------------------------------------------------------------------
// Exported length helpers (varchar.c:669)
// ---------------------------------------------------------------------------

/// `bcTruelen` — true length of a BpChar (varchar.c:669): the length not
/// counting trailing blanks. `arg` is the detoasted VARDATA payload. Delegates
/// to `backend-utils-adt-varlena`'s ported `bpchartruelen`.
fn bcTruelen(arg: &[u8]) -> i32 {
    bpchartruelen(arg) as i32
}

/// `bpcharlen` — character length of a `char(n)` (varchar.c:692). `arg` is the
/// detoasted VARDATA payload.
pub fn bpcharlen(arg: &[u8]) -> PgResult<i32> {
    // get number of bytes, ignoring trailing spaces
    let mut len = bcTruelen(arg);

    // in multibyte encoding, convert to number of characters
    if mb::pg_database_encoding_max_length::call() != 1 {
        len = mb::pg_mbstrlen_with_len::call(&arg[..len as usize], len)?;
    }

    Ok(len)
}

/// `bpcharoctetlen` — byte length of a `char(n)` (varchar.c:708).
///
/// In C this is `toast_raw_datum_size(arg) - VARHDRSZ` and need not detoast the
/// input. `raw_total_size` is the raw datum size (VARHDRSZ + the stored bytes),
/// i.e. `toast_raw_datum_size`; the caller supplies it without detoasting.
pub fn bpcharoctetlen(raw_total_size: usize) -> i32 {
    raw_total_size as i32 - VARHDRSZ
}

// ---------------------------------------------------------------------------
// bpchar comparison / hashing  (varchar.c:726)
// ---------------------------------------------------------------------------

#[inline]
fn OidIsValid(collid: Oid) -> bool {
    collid != InvalidOid
}

/// `check_collation_set` (varchar.c:726) — the bpchar comparison-path variant.
/// Delegates to varlena's ported `check_collation_set` (identical text), but C
/// has two copies; keep this thin wrapper so the call sites mirror C exactly.
fn check_collation_set_local(collid: Oid) -> PgResult<()> {
    if !OidIsValid(collid) {
        // This typically means that the parser could not resolve a conflict of
        // implicit collations, so report it that way.
        return Err(PgError::error(
            "could not determine which collation to use for string comparison",
        )
        .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
        .with_hint("Use the COLLATE clause to set the collation explicitly."));
    }
    // Use varlena's shared checker for parity (same condition/message); the call
    // above already raised on InvalidOid, so this is unreachable on the error
    // path and a no-op on success.
    check_collation_set(collid)
}

/// `bpchareq` (varchar.c:742). `arg1`/`arg2` are detoasted VARDATA payloads.
pub fn bpchareq(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    check_collation_set_local(collid)?;

    let len1 = bcTruelen(arg1);
    let len2 = bcTruelen(arg2);

    let result = if loc::collation_is_deterministic::call(collid)? {
        // Since we only care about equality or not-equality, we can avoid all
        // the expense of strcoll() here, and just do bitwise comparison.
        if len1 != len2 {
            false
        } else {
            arg1[..len1 as usize] == arg2[..len2 as usize]
        }
    } else {
        varstr_cmp(&arg1[..len1 as usize], &arg2[..len2 as usize], collid)? == 0
    };
    Ok(result)
}

/// `bpcharne` (varchar.c:783).
pub fn bpcharne(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    check_collation_set_local(collid)?;

    let len1 = bcTruelen(arg1);
    let len2 = bcTruelen(arg2);

    let result = if loc::collation_is_deterministic::call(collid)? {
        if len1 != len2 {
            true
        } else {
            arg1[..len1 as usize] != arg2[..len2 as usize]
        }
    } else {
        varstr_cmp(&arg1[..len1 as usize], &arg2[..len2 as usize], collid)? != 0
    };
    Ok(result)
}

/// Shared body of the bpchar ordering operators: `varstr_cmp` of the truncated
/// values. Used by lt/le/gt/ge/cmp/larger/smaller (varchar.c:824-980).
fn bpchar_varstr_cmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    let len1 = bcTruelen(arg1);
    let len2 = bcTruelen(arg2);
    varstr_cmp(&arg1[..len1 as usize], &arg2[..len2 as usize], collid)
}

/// `bpcharlt` (varchar.c:824).
pub fn bpcharlt(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpchar_varstr_cmp(arg1, arg2, collid)? < 0)
}

/// `bpcharle` (varchar.c:845).
pub fn bpcharle(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpchar_varstr_cmp(arg1, arg2, collid)? <= 0)
}

/// `bpchargt` (varchar.c:866).
pub fn bpchargt(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpchar_varstr_cmp(arg1, arg2, collid)? > 0)
}

/// `bpcharge` (varchar.c:887).
pub fn bpcharge(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpchar_varstr_cmp(arg1, arg2, collid)? >= 0)
}

/// `bpcharcmp` (varchar.c:908).
pub fn bpcharcmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    bpchar_varstr_cmp(arg1, arg2, collid)
}

/// `bpchar_sortsupport` (varchar.c:929). Installs the generic string SortSupport
/// for `BPCHAROID` / `ssup->ssup_collation`. The C `MemoryContextSwitchTo(
/// ssup->ssup_cxt)` wrap is owned by varlena's `varstr_sortsupport` (it charges
/// its scratch to `ssup.ssup_cxt`).
pub fn bpchar_sortsupport<'mcx>(
    ssup: &mut SortSupportData<'mcx>,
) -> PgResult<backend_utils_adt_varlena::sortsupport::VarStrSortSupport<'mcx>> {
    let collid = ssup.ssup_collation;
    // Use generic string SortSupport
    varstr_sortsupport(ssup, BPCHAROID, collid)
}

/// `bpchar_larger` (varchar.c:946). Returns true if `arg1` should be returned
/// (`cmp >= 0`), false to return `arg2`; mirrors `PG_RETURN_BPCHAR_P((cmp >= 0)
/// ? arg1 : arg2)`. The caller returns the corresponding source value.
pub fn bpchar_larger(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpchar_varstr_cmp(arg1, arg2, collid)? >= 0)
}

/// `bpchar_smaller` (varchar.c:964). Returns true if `arg1` should be returned
/// (`cmp <= 0`), false to return `arg2`.
pub fn bpchar_smaller(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpchar_varstr_cmp(arg1, arg2, collid)? <= 0)
}

/// Build the indeterminate-collation error for hashing (varchar.c:998/1054).
fn hash_collation_error() -> PgError {
    PgError::error("could not determine which collation to use for string hashing")
        .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
        .with_hint("Use the COLLATE clause to set the collation explicitly.")
}

/// `hashbpchar` (varchar.c:987). `key` is the detoasted VARDATA payload.
pub fn hashbpchar<'mcx>(mcx: Mcx<'mcx>, key: &[u8], collid: Oid) -> PgResult<u32> {
    if collid == 0 {
        return Err(hash_collation_error());
    }

    let keylen = bcTruelen(key) as usize;

    if loc::collation_is_deterministic::call(collid)? {
        Ok(hash_bytes(&key[..keylen]))
    } else {
        // C: pg_strnxfrm into a palloc'd buffer, hash bsize+1 bytes (transform +
        // a single appended trailing NUL), then pfree. The repo's `pg_strxfrm`
        // returns the complete transformed blob charged to `mcx`.
        //
        // In principle there's no reason to include the terminating NUL in the
        // hash, but it was done before and the behavior must be preserved.
        let transform = loc::pg_strxfrm::call(mcx, collid, &key[..keylen])?;
        let mut withnul = mcx::vec_with_capacity_in(mcx, transform.len() + 1)?;
        withnul.extend_from_slice(&transform);
        withnul.push(0);
        Ok(hash_bytes(&withnul))
    }
}

/// `hashbpcharextended` (varchar.c:1043).
pub fn hashbpcharextended<'mcx>(
    mcx: Mcx<'mcx>,
    key: &[u8],
    collid: Oid,
    seed: u64,
) -> PgResult<u64> {
    if collid == 0 {
        return Err(hash_collation_error());
    }

    let keylen = bcTruelen(key) as usize;

    if loc::collation_is_deterministic::call(collid)? {
        Ok(hash_bytes_extended(&key[..keylen], seed))
    } else {
        // See `hashbpchar`: hash the transform plus one appended trailing NUL.
        let transform = loc::pg_strxfrm::call(mcx, collid, &key[..keylen])?;
        let mut withnul = mcx::vec_with_capacity_in(mcx, transform.len() + 1)?;
        withnul.extend_from_slice(&transform);
        withnul.push(0);
        Ok(hash_bytes_extended(&withnul, seed))
    }
}

// ---------------------------------------------------------------------------
// pattern-ordering operators (text_pattern_ops family for bpchar) (varchar.c:1108)
// ---------------------------------------------------------------------------

/// `internal_bpchar_pattern_compare` — the raw memcmp-style comparator
/// (varchar.c:1108). `arg1`/`arg2` are detoasted VARDATA payloads.
pub fn internal_bpchar_pattern_compare(arg1: &[u8], arg2: &[u8]) -> i32 {
    let len1 = bcTruelen(arg1);
    let len2 = bcTruelen(arg2);

    let n = len1.min(len2) as usize;
    let result = memcmp(&arg1[..n], &arg2[..n]);
    if result != 0 {
        result
    } else if len1 < len2 {
        -1
    } else if len1 > len2 {
        1
    } else {
        0
    }
}

/// `bpchar_pattern_lt` (varchar.c:1130).
pub fn bpchar_pattern_lt(arg1: &[u8], arg2: &[u8]) -> bool {
    internal_bpchar_pattern_compare(arg1, arg2) < 0
}

/// `bpchar_pattern_le` (varchar.c:1146).
pub fn bpchar_pattern_le(arg1: &[u8], arg2: &[u8]) -> bool {
    internal_bpchar_pattern_compare(arg1, arg2) <= 0
}

/// `bpchar_pattern_ge` (varchar.c:1162).
pub fn bpchar_pattern_ge(arg1: &[u8], arg2: &[u8]) -> bool {
    internal_bpchar_pattern_compare(arg1, arg2) >= 0
}

/// `bpchar_pattern_gt` (varchar.c:1178).
pub fn bpchar_pattern_gt(arg1: &[u8], arg2: &[u8]) -> bool {
    internal_bpchar_pattern_compare(arg1, arg2) > 0
}

/// `btbpchar_pattern_cmp` (varchar.c:1194).
pub fn btbpchar_pattern_cmp(arg1: &[u8], arg2: &[u8]) -> i32 {
    internal_bpchar_pattern_compare(arg1, arg2)
}

/// `btbpchar_pattern_sortsupport` (varchar.c:1210). Like `bpchar_sortsupport`
/// but forces the `"C"` collation.
pub fn btbpchar_pattern_sortsupport<'mcx>(
    ssup: &mut SortSupportData<'mcx>,
) -> PgResult<backend_utils_adt_varlena::sortsupport::VarStrSortSupport<'mcx>> {
    // Use generic string SortSupport, forcing "C" collation
    varstr_sortsupport(ssup, BPCHAROID, C_COLLATION_OID)
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

/// Build a NUL-terminated cstring (`PG_RETURN_CSTRING` analog): the payload
/// bytes followed by exactly one trailing NUL, charged to `mcx`.
fn cstring<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = mcx::vec_with_capacity_in(mcx, bytes.len() + 1)?;
    out.extend_from_slice(bytes);
    out.push(0);
    Ok(out)
}

/// `memcmp(a, b, n)` over equal-length slices, returning the C three-way result
/// sign (the first differing *unsigned* byte's difference, or 0).
fn memcmp(a: &[u8], b: &[u8]) -> i32 {
    for (x, y) in a.iter().zip(b.iter()) {
        if x != y {
            return *x as i32 - *y as i32;
        }
    }
    0
}

/// `value too long for type character(%d)` (STRING_DATA_RIGHT_TRUNCATION).
fn value_too_long_character(maxlen: i32) -> PgError {
    PgError::error(format!("value too long for type character({maxlen})"))
        .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
}

/// `value too long for type character varying(%d)` (STRING_DATA_RIGHT_TRUNCATION).
fn value_too_long_varying(maxlen: i32) -> PgError {
    PgError::error(format!(
        "value too long for type character varying({maxlen})"
    ))
    .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
}

#[cfg(test)]
mod tests;
