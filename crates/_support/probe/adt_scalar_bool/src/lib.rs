// NB: not `#![no_std]` — the fmgr builtin registration layer (`fmgr_builtins`)
// registers the `bool.c` builtins into the fmgr-core table (C: `fmgr_builtins[]`),
// which uses `String`/`std` (`std::panic::panic_any` for the `ereport` bridge).
#![allow(clippy::result_large_err)]

//! Port of PostgreSQL 18.3 `src/backend/utils/adt/bool.c`: the built-in
//! `boolean` type — its parser, text/binary I/O, comparison operators, hash
//! functions, and the `bool_and`/`bool_or` aggregate support functions.
//!
//! The fmgr/`Datum` marshalling layer (argument decode, registry rows, the
//! `PG_FUNCTION_ARGS` boundary) is not part of this unit; like the sibling
//! adt ports (`backend-utils-adt-numutils`) these are plain typed Rust
//! functions. Pass-by-value `bool` arguments arrive as `bool`; `cstring`
//! arrives as `&str`; the `internal` aggregate-state pointer is the owned
//! [`BoolAggState`].
//!
//! Calls into other units that would form a dependency cycle go through that
//! unit's seam crate: `cstring_to_text` (varlena). `hash_bytes_uint32{,_extended}`
//! and the `pqformat` send/recv helpers are non-cyclic and called directly.

extern crate alloc;

mod fmgr_builtins;

use alloc::format;

use pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgbyte, pq_sendbyte};
use ::varlena_seams::cstring_to_text;
use hashfn::{hash_bytes_uint32, hash_bytes_uint32_extended};
use ::mcx::Mcx;
use datum::{Bytea, Datum};
use types_error::{PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION};
use ::stringinfo::StringInfo;

// ===========================================================================
// Pure parser (bool.c:35-160)
// ===========================================================================

/// `pg_strncasecmp(value, lit, n) == 0` restricted to the ASCII literals
/// `parse_bool_with_len` compares against (`src/port/pgstrcasecmp.c`). `value`
/// is the trimmed input (exactly its byte length is available); `lit` is the
/// NUL-terminated C literal. C breaks early once both strings reach the
/// terminating NUL, modelled here by reading a `0` byte past the end of either
/// slice.
fn pg_strncasecmp_eq(value: &[u8], lit: &[u8], n: usize) -> bool {
    let mut i = 0;
    while i < n {
        // `value` carries exactly its own bytes; reading past it is C reading
        // the input cstring's terminating NUL.
        let ch1 = value.get(i).copied().unwrap_or(0);
        // `lit` is NUL-terminated; reading past its bytes yields the NUL.
        let ch2 = lit.get(i).copied().unwrap_or(0);

        if ch1 != ch2 {
            let l1 = to_lower_ascii(ch1);
            let l2 = to_lower_ascii(ch2);
            if l1 != l2 {
                return false;
            }
        }
        if ch1 == 0 {
            break;
        }
        i += 1;
    }
    true
}

/// ASCII `A`..`Z` → `a`..`z`, mirroring the `ch >= 'A' && ch <= 'Z'` arm of
/// `pg_strncasecmp`. (The high-bit `isupper`/`tolower` locale arm never applies
/// to the literals `parse_bool_with_len` compares against.)
fn to_lower_ascii(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch + (b'a' - b'A')
    } else {
        ch
    }
}

/// `parse_bool` (bool.c:35): `parse_bool_with_len(value, strlen(value), result)`.
///
/// Returns `Some(value)` on a recognized boolean spelling, `None` otherwise.
pub fn parse_bool(value: &str) -> Option<bool> {
    parse_bool_with_len(value, value.len())
}

/// `parse_bool_with_len` (bool.c:42): recognize a boolean spelling. Valid
/// values are `true`, `false`, `yes`, `no`, `on`, `off`, `1`, `0`, as well as
/// unique prefixes thereof.
///
/// `len` is the number of leading bytes of `value` to consider (the caller's
/// trimmed length); `value` must have at least `len` bytes. Returns
/// `Some(true)`/`Some(false)` for a recognized spelling, `None` otherwise. The
/// branch order, the `'o'`-needs-≥2-chars special case, and the single-char
/// `'1'`/`'0'` cases are preserved exactly from C.
pub fn parse_bool_with_len(value: &str, len: usize) -> Option<bool> {
    let bytes = value.as_bytes();
    // C switches on `*value`; an empty string makes `*value` the NUL byte,
    // which falls through to the default (no match).
    let first = bytes.first().copied().unwrap_or(0);
    match first {
        b't' | b'T' => {
            if pg_strncasecmp_eq(bytes, b"true", len) {
                return Some(true);
            }
        }
        b'f' | b'F' => {
            if pg_strncasecmp_eq(bytes, b"false", len) {
                return Some(false);
            }
        }
        b'y' | b'Y' => {
            if pg_strncasecmp_eq(bytes, b"yes", len) {
                return Some(true);
            }
        }
        b'n' | b'N' => {
            if pg_strncasecmp_eq(bytes, b"no", len) {
                return Some(false);
            }
        }
        b'o' | b'O' => {
            // 'o' is not unique enough: compare at least 2 chars.
            let n = if len > 2 { len } else { 2 };
            if pg_strncasecmp_eq(bytes, b"on", n) {
                return Some(true);
            } else if pg_strncasecmp_eq(bytes, b"off", n) {
                return Some(false);
            }
        }
        b'1' => {
            if len == 1 {
                return Some(true);
            }
        }
        b'0' => {
            if len == 1 {
                return Some(false);
            }
        }
        _ => {}
    }
    None
}

// ===========================================================================
// USER I/O ROUTINES (bool.c:111-217)
// ===========================================================================

/// ASCII `isspace`: space, `\t`, `\n`, `\v`, `\f`, `\r` — matching C `isspace`
/// over the "C" locale on `unsigned char` input.
fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// `boolin` (bool.c:111): input function for type `boolean` — `cstring` →
/// `bool`. Skips leading/trailing ASCII whitespace, then
/// [`parse_bool_with_len`]. On a bad spelling routes
/// `errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)` through `escontext` (soft) or
/// throws (hard). The error message embeds the *original* untrimmed input,
/// exactly as C does (`in_str`).
pub fn boolin(in_str: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<bool> {
    let bytes = in_str.as_bytes();

    // Skip leading whitespace.
    let mut start = 0;
    while start < bytes.len() && is_space(bytes[start]) {
        start += 1;
    }

    // Trailing whitespace: shrink `len` (relative to `start`).
    let mut len = bytes.len() - start;
    while len > 0 && is_space(bytes[start + len - 1]) {
        len -= 1;
    }

    // `str` is the trimmed view; `len` bytes of it are significant.
    let trimmed = &in_str[start..start + len];

    if let Some(result) = parse_bool_with_len(trimmed, len) {
        return Ok(result);
    }

    // C `ereturn(escontext, (Datum) 0, …)`: false is the suppress-warning
    // value the C parser leaves in `*result`.
    ::types_error::ereturn(
        escontext,
        false,
        PgError::error(format!(
            "invalid input syntax for type {}: \"{}\"",
            "boolean", in_str
        ))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

/// `boolout` (bool.c:137): converts 1 or 0 to `"t"` or `"f"`.
pub fn boolout(b: bool) -> &'static str {
    if b {
        "t"
    } else {
        "f"
    }
}

/// `boolrecv` (bool.c:154): external binary format → `bool`. The external
/// representation is one byte; any nonzero value is taken as `true`.
pub fn boolrecv(buf: &mut StringInfo<'_>) -> PgResult<bool> {
    let ext = pq_getmsgbyte(buf)?;
    Ok(ext != 0)
}

/// `boolsend` (bool.c:170): `bool` → binary format (a one-byte `bytea`).
pub fn boolsend(mcx: Mcx<'_>, arg1: bool) -> PgResult<Bytea<'_>> {
    let mut buf = pq_begintypsend(mcx)?;
    pq_sendbyte(&mut buf, if arg1 { 1 } else { 0 })?;
    Ok(pq_endtypsend(buf))
}

/// `booltext` (bool.c:204): cast function for `bool` => `text`. Distinct from
/// [`boolout`] (`"t"`/`"f"`): this follows the SQL-spec result (`"true"` /
/// `"false"`, except for producing lower case). C `cstring_to_text` palloc's
/// the `text` image in the current context; here it routes through the varlena
/// seam, returning the `Datum` the caller passes on (`PG_RETURN_TEXT_P`).
pub fn booltext(mcx: Mcx<'_>, arg1: bool) -> PgResult<Datum> {
    let str = if arg1 { "true" } else { "false" };
    cstring_to_text::call(mcx, str)
}

// ===========================================================================
// PUBLIC ROUTINES — comparison operators (bool.c:224-279)
// ===========================================================================

/// `booleq` (bool.c:224): `arg1 == arg2`.
pub fn booleq(arg1: bool, arg2: bool) -> bool {
    arg1 == arg2
}

/// `boolne` (bool.c:232): `arg1 != arg2`.
pub fn boolne(arg1: bool, arg2: bool) -> bool {
    arg1 != arg2
}

/// `boollt` (bool.c:241): `arg1 < arg2` (false < true).
pub fn boollt(arg1: bool, arg2: bool) -> bool {
    !arg1 & arg2
}

/// `boolgt` (bool.c:250): `arg1 > arg2`.
pub fn boolgt(arg1: bool, arg2: bool) -> bool {
    arg1 & !arg2
}

/// `boolle` (bool.c:259): `arg1 <= arg2`.
pub fn boolle(arg1: bool, arg2: bool) -> bool {
    arg1 <= arg2
}

/// `boolge` (bool.c:268): `arg1 >= arg2`.
pub fn boolge(arg1: bool, arg2: bool) -> bool {
    arg1 >= arg2
}

// ===========================================================================
// Hash functions (bool.c:277-291)
// ===========================================================================

/// `hashbool` (bool.c:277): `hash_uint32((int32) arg)`. The bool is widened to
/// `int32` (0/1) before hashing, exactly as C `(int32) PG_GETARG_BOOL(0)`.
/// C `hash_uint32(k)` is `UInt32GetDatum(hash_bytes_uint32(k))`.
pub fn hashbool(arg: bool) -> Datum {
    Datum::from_u32(hash_bytes_uint32(arg as i32 as u32))
}

/// `hashboolextended` (bool.c:285): `hash_uint32_extended((int32) arg, seed)`.
/// C `hash_uint32_extended(k, seed)` is
/// `UInt64GetDatum(hash_bytes_uint32_extended(k, seed))`.
pub fn hashboolextended(arg: bool, seed: i64) -> Datum {
    Datum::from_u64(hash_bytes_uint32_extended(arg as i32 as u32, seed as u64))
}

// ===========================================================================
// boolean-and / boolean-or aggregates (bool.c:293-456)
// ===========================================================================

/// `booland_statefunc` (bool.c:303): standard EVERY / `bool_and` transition.
/// Plain aggregate mode only (not moving-aggregate). `arg1 && arg2`.
pub fn booland_statefunc(arg1: bool, arg2: bool) -> bool {
    arg1 && arg2
}

/// `boolor_statefunc` (bool.c:316): standard ANY/SOME / `bool_or` transition.
/// Plain aggregate mode only (not moving-aggregate). `arg1 || arg2`.
pub fn boolor_statefunc(arg1: bool, arg2: bool) -> bool {
    arg1 || arg2
}

/// `BoolAggState` (bool.c:321): the transition state for `bool_accum`. In the C
/// it is `MemoryContextAlloc`'d in the aggregate context and threaded through
/// the `internal`-typed state argument; here it is an owned struct the caller
/// keeps across transitions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BoolAggState {
    /// number of non-null values aggregated
    pub aggcount: i64,
    /// number of values aggregated that are true
    pub aggtrue: i64,
}

/// `makeBoolAggState` (bool.c:327): allocate and zero a fresh [`BoolAggState`].
///
/// C calls `AggCheckCallContext(fcinfo, &agg_context)` first and
/// `elog(ERROR, "aggregate function called in non-aggregate context")` when it
/// returns 0, then `MemoryContextAlloc`s the state in that context. The owned
/// model carries the resolved aggregate context as `agg_context`; `None` is the
/// C "not in aggregate context" case and raises the same error. The state is a
/// plain by-value struct, so no context allocation is needed once the check
/// passes.
pub fn make_bool_agg_state(agg_context: Option<Mcx<'_>>) -> PgResult<BoolAggState> {
    if agg_context.is_none() {
        // C `elog(ERROR, …)` — an internal (XX000) hard error.
        return Err(PgError::error(
            "aggregate function called in non-aggregate context",
        ));
    }
    Ok(BoolAggState {
        aggcount: 0,
        aggtrue: 0,
    })
}

/// `bool_accum` (bool.c:344): forward transition. Creates the state on first
/// call (when `state` is `None`, C's NULL `internal` arg). A null `value`
/// (`PG_ARGISNULL(1)`) is skipped; otherwise it bumps `aggcount` and, when
/// true, `aggtrue`. Returns the (updated) state.
pub fn bool_accum(
    agg_context: Option<Mcx<'_>>,
    state: Option<BoolAggState>,
    value: Option<bool>,
) -> PgResult<BoolAggState> {
    let mut state = match state {
        Some(s) => s,
        None => make_bool_agg_state(agg_context)?,
    };

    if let Some(v) = value {
        state.aggcount += 1;
        if v {
            state.aggtrue += 1;
        }
    }

    Ok(state)
}

/// `bool_accum_inv` (bool.c:366): inverse transition (moving aggregate). The
/// state must already exist (C `elog(ERROR, "bool_accum_inv called with NULL
/// state")`). A null `value` is skipped; otherwise it decrements `aggcount`
/// and, when true, `aggtrue`. Returns the updated state.
pub fn bool_accum_inv(state: Option<BoolAggState>, value: Option<bool>) -> PgResult<BoolAggState> {
    let mut state = match state {
        Some(s) => s,
        // C `elog(ERROR, …)` — an internal (XX000) hard error.
        None => return Err(PgError::error("bool_accum_inv called with NULL state")),
    };

    if let Some(v) = value {
        state.aggcount -= 1;
        if v {
            state.aggtrue -= 1;
        }
    }

    Ok(state)
}

/// `bool_alltrue` (bool.c:388): `bool_and` / `every` final function. Returns
/// `None` (SQL NULL) when there were no non-null values; otherwise `true` iff
/// all non-null values were true.
pub fn bool_alltrue(state: Option<BoolAggState>) -> Option<bool> {
    match state {
        None => None,
        Some(s) if s.aggcount == 0 => None,
        Some(s) => Some(s.aggtrue == s.aggcount),
    }
}

/// `bool_anytrue` (bool.c:404): `bool_or` final function. Returns `None` (SQL
/// NULL) when there were no non-null values; otherwise `true` iff any non-null
/// value was true.
pub fn bool_anytrue(state: Option<BoolAggState>) -> Option<bool> {
    match state {
        None => None,
        Some(s) if s.aggcount == 0 => None,
        Some(s) => Some(s.aggtrue > 0),
    }
}

/// Installs the `parse_bool` seam declared in
/// `backend-utils-adt-scalar-seams`. The seam's nominal owner is the
/// `backend-utils-adt-scalar` unit (`bool.c`/`datum.c`/`oid.c`/…), still
/// unported as one crate; `bool.c` itself is fully ported here, so this crate
/// is `parse_bool`'s real home and installs it for the GUC / walsender
/// `replication=...` consumers (`backend-tcop-backend-startup`). The sibling
/// `datum_copy` seam in that same crate is installed by its own owner
/// (`backend-utils-adt-scalar-datum-core`).
pub fn init_seams() {
    scalar_seams::parse_bool::set(parse_bool);
    // Register the `bool.c` builtins into the fmgr-core table (C: their
    // `fmgr_builtins[]` rows), so by-OID fmgr dispatch resolves them.
    fmgr_builtins::register_probe_adt_scalar_bool_builtins();
}

#[cfg(test)]
mod tests;
