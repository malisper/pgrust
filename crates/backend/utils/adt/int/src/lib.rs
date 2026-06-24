//! Idiomatic port of `src/backend/utils/adt/int.c` -- the built-in `int2`
//! (smallint) and `int4` (integer) types.
//!
//! Every scalar `int.c` function is ported with the original C name and
//! logic/branch-order/message-text/SQLSTATE preserved 1:1.  Arithmetic uses the
//! signed-overflow primitives from `common/int.h` ([`overflow`], faithful
//! ports as plain Rust); parsing/formatting reuse the idiomatic
//! `backend-utils-adt-numutils` (`pg_strtoint16_safe` / `pg_strtoint32_safe` /
//! `pg_itoa` / `pg_ltoa`).  All overflow / divide / range errors carry the
//! exact C message text and SQLSTATE through `backend-utils-error`.
//!
//! The pure cores take/return plain `i16`/`i32`/`bool` (the C `fmgr` /
//! `PG_FUNCTION_ARGS` / `Datum` boundary is the project-wide systemic deferral
//! and is dropped here, exactly as in the sibling idiomatic adt crates).  The
//! binary `recv`/`send` (`int2recv`/`int4recv`/`int2send`/`int4send`) call the
//! ported `libpq/pqformat.c` framing primitives over the real value-typed
//! `StringInfo`/`Bytea`/`Mcx`; the int.c-specific narrowing casts stay in-crate.
//!
//! The `generate_series_int4` SRF cross-call state and per-call step are PURE
//! and ported in-crate ([`GenerateSeriesInt4`]); only the funcapi `SRF_*` glue
//! (the deferred fmgr layer) is omitted.  The planner-support row estimate
//! ([`generate_series_int4_rows`]) is ported; classifying the `Const`/NULL
//! argument nodes is the optimizer/`nodes` layer's job, fed in as resolved
//! `f64`s.
//!
//! The `int2vector` I/O (`buildint2vector`, `int2vectorin`, `int2vectorout`)
//! builds and reads the on-disk `int2vector` image -- a 1-D `ArrayType` of
//! `INT2OID` (2-byte pass-by-value, short-aligned), lower bound 0, no NULLs --
//! through the array subsystem's `construct_md_array` /
//! `int2vector_to_i16s_bytes`, and is registered here.  The binary
//! `int2vectorrecv`/`int2vectorsend` still need the `array_recv`/`array_send`
//! fcinfo-sharing path (they reuse the caller's `flinfo->fn_extra` cache) and so
//! remain unregistered; they will land with that array machinery rather than be
//! faked.  [`check_valid_int2vector`] validates an already-decoded array header
//! (its seam takes the header fields, not the carrier), as `int2vectorout`
//! consumes it.  `int2vector` has no btree/hash opclass in PostgreSQL, so there
//! are no `int2vector` comparison operators to register (unlike `oidvector`).
//!
//! No `extern "C"`, no `*mut`/`*const`, no `libc`; soft errors flow through
//! `backend-utils-error`.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
// `clippy::manual_range_contains`: the range checks are written as the exact C
// predicate `arg < MIN || arg > MAX` (e.g. `i4toi2`, int.c:381) so the source
// reads 1:1 against the C; rewriting to `!(MIN..=MAX).contains(&arg)` would
// obscure the correspondence.  Accepted crate-wide, as in the sibling
// `backend-utils-adt-float`.
#![allow(clippy::manual_range_contains)]

pub mod fmgr_builtins;
pub mod overflow;
pub mod series;

/// This unit owns no inward `-seams` crate (its value cores are consumed
/// directly). `init_seams()` registers the `int.c` fmgr builtins into the
/// fmgr-core builtin table so `fmgr_isbuiltin` resolves them on the fast path
/// (catalog scankeys need `int4eq`/`int2eq`/... before any catalog access).
pub fn init_seams() {
    fmgr_builtins::register_int_builtins();
}

pub use overflow::{
    pg_add_s16_overflow, pg_add_s32_overflow, pg_add_s64_overflow, pg_mul_s16_overflow,
    pg_mul_s32_overflow, pg_mul_s64_overflow, pg_sub_s16_overflow, pg_sub_s32_overflow,
    pg_sub_s64_overflow,
};
pub use series::{generate_series_int4_rows, GenerateSeriesInt4};

use ::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_sendint16, pq_sendint32,
};
use ::numutils::{pg_itoa, pg_ltoa, pg_strtoint16_safe, pg_strtoint32_safe};
use ::utils_error::{ereport, PgError, PgResult, SoftErrorContext};
use ::mcx::Mcx;
use ::datum::Bytea;
use ::types_error::{
    ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR,
};
use ::stringinfo::StringInfo;

use overflow::{pg_add_s32_overflow as add_s32, pg_add_s64_overflow as add_s64};

// `SHRT_MIN`/`SHRT_MAX`/`PG_INT16_MIN`/`PG_INT32_MIN`.
const SHRT_MIN: i32 = i16::MIN as i32;
const SHRT_MAX: i32 = i16::MAX as i32;
const PG_INT16_MIN: i16 = i16::MIN;
const PG_INT32_MIN: i32 = i32::MIN;

/// `ereport(ERROR, errcode(22003), errmsg("integer out of range"))`.
fn integer_out_of_range() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .errmsg("integer out of range")
        .into_error()
}

/// `ereport(ERROR, errcode(22003), errmsg("smallint out of range"))`.
fn smallint_out_of_range() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .errmsg("smallint out of range")
        .into_error()
}

/// `ereport(ERROR, errcode(22012), errmsg("division by zero"))`.
fn division_by_zero() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_DIVISION_BY_ZERO)
        .errmsg("division by zero")
        .into_error()
}

fn invalid_preceding_following() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
        .errmsg("invalid preceding or following size in window function")
        .into_error()
}

/// Render an int16 to its decimal string (`pg_itoa` into a worst-case buffer:
/// sign + 5 digits + NUL).
fn itoa_string(arg1: i16) -> String {
    let mut buf = [0u8; 8];
    let len = pg_itoa(arg1, &mut buf);
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

/// Render an int32 to its decimal string (`pg_ltoa` into a worst-case buffer:
/// sign + 10 digits + NUL).
fn ltoa_string(arg1: i32) -> String {
    let mut buf = [0u8; 13];
    let len = pg_ltoa(arg1, &mut buf);
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

// ===========================================================================
// USER I/O ROUTINES (int2)
// ===========================================================================

/// `int2in()` (int.c:62): cstring -> int16 via `pg_strtoint16_safe`.
pub fn int2in(num: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i16> {
    pg_strtoint16_safe(num, escontext)
}

/// `int2out()` (int.c:73): int16 -> "num" (`pg_itoa`).
pub fn int2out(arg1: i16) -> String {
    itoa_string(arg1)
}

/// `int2recv()` (int.c:86): `(int16) pq_getmsgint(buf, sizeof(int16))`.
pub fn int2recv(buf: &mut StringInfo<'_>) -> PgResult<i16> {
    Ok(pq_getmsgint(buf, core::mem::size_of::<i16>() as i32)? as u16 as i16)
}

/// `int2send()` (int.c:97): `pq_sendint16(&buf, arg1)`.
pub fn int2send<'mcx>(mcx: Mcx<'mcx>, arg1: i16) -> PgResult<Bytea<'mcx>> {
    let mut buf = pq_begintypsend(mcx)?;
    pq_sendint16(&mut buf, arg1 as u16)?;
    Ok(pq_endtypsend(buf))
}

// ===========================================================================
// int2vector I/O ROUTINES
// ===========================================================================

/// `buildint2vector(int2s, n)` (int.c:107): build the `int2vector` on-disk image
/// -- a 1-D `ArrayType` of `INT2OID` (2-byte pass-by-value, short-aligned, no
/// NULLs) whose index lower bound is 0 (not 1), matching the historical
/// int2vector layout. An empty input yields a zero-dimension array. If `int2s`
/// is `None` the caller fills the values afterward (C leaves them zeroed); we
/// build directly from the provided slice instead.
pub fn buildint2vector<'mcx>(
    mcx: Mcx<'mcx>,
    int2s: &[i16],
) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    // construct_md_array(elems, NULL, 1, &dim1, &lbound0, INT2OID, sizeof(int16),
    //                    true /* byval */, TYPALIGN_SHORT)
    let datums: Vec<::datum::Datum> =
        int2s.iter().map(|&v| ::datum::Datum::from_i16(v)).collect();
    let n = int2s.len() as i32;
    arrayfuncs::construct::construct_md_array(
        mcx,
        &datums,
        None,
        1,
        &[n],
        &[0], // lbound 0, per int2vector convention
        types_core::INT2OID,
        core::mem::size_of::<i16>() as i32,
        true,
        b's', // TYPALIGN_SHORT
    )
}

/// `check_valid_int2vector` (int.c:144): validate that an array object meets the
/// `int2vector` restrictions -- `ndim == 1`, `dataoffset == 0` (no nulls), and
/// `elemtype == INT2OID`. A violation is
/// `ereport(ERROR, ERRCODE_DATATYPE_MISMATCH, "array is not a valid int2vector")`.
///
/// The array header is already decoded by the caller (the carrier lives in the
/// array subsystem), so this takes the three checked header fields.
pub fn check_valid_int2vector(ndim: i32, dataoffset: i32, elemtype: types_core::Oid) -> PgResult<()> {
    if ndim != 1 || dataoffset != 0 || elemtype != types_core::INT2OID {
        return Err(PgError::error("array is not a valid int2vector")
            .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH));
    }
    Ok(())
}

/// `int2vectorin` (int.c:166): parse a whitespace-separated list of smallints
/// into an `int2vector` image. The C uses `strtol` directly with `"smallint"`
/// range checks (`SHRT_MIN..SHRT_MAX`). A soft parse error (bad token /
/// out-of-range) records into `escontext` and returns `None` (C's
/// `ereturn(escontext, (Datum) 0, ...)`); a hard error propagates as `Err`.
pub fn int2vectorin<'mcx>(
    mcx: Mcx<'mcx>,
    input: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<::mcx::PgVec<'mcx, u8>>> {
    let mut ints: Vec<i16> = Vec::new();
    let mut rest = input;
    loop {
        // while (*intString && isspace((unsigned char) *intString)) intString++;
        rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());
        // if (*intString == '\0') break;
        if rest.is_empty() {
            break;
        }
        // l = strtol(intString, &endp, 10);
        let (l, consumed) = strtol_base10(rest);
        // if (intString == endp) -> invalid input syntax
        if consumed == 0 {
            return soft_error_or_err(
                escontext,
                PgError::error(format!(
                    "invalid input syntax for type {}: \"{}\"",
                    "smallint", rest
                ))
                .with_sqlstate(::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
            );
        }
        // if (errno == ERANGE || l < SHRT_MIN || l > SHRT_MAX) -> out of range
        if l < i16::MIN as i64 || l > i16::MAX as i64 {
            return soft_error_or_err(
                escontext,
                PgError::error(format!(
                    "value \"{}\" is out of range for type {}",
                    rest, "smallint"
                ))
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
            );
        }
        // if (*endp && *endp != ' ') -> invalid input syntax
        let endp = &rest[consumed..];
        let next = endp.chars().next();
        if let Some(c) = next {
            if c != ' ' {
                return soft_error_or_err(
                    escontext,
                    PgError::error(format!(
                        "invalid input syntax for type {}: \"{}\"",
                        "smallint", rest
                    ))
                    .with_sqlstate(::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
                );
            }
        }
        ints.push(l as i16);
        rest = endp;
    }
    Ok(Some(buildint2vector(mcx, &ints)?))
}

/// Either records the error into the soft `escontext` and returns `Ok(None)`
/// (C's `ereturn(escontext, (Datum) 0, ...)`), or propagates it as a hard error
/// (`escontext == NULL`).
fn soft_error_or_err<'mcx>(
    escontext: Option<&mut SoftErrorContext>,
    err: PgError,
) -> PgResult<Option<::mcx::PgVec<'mcx, u8>>> {
    match escontext {
        Some(ctx) => {
            ctx.save(err);
            Ok(None)
        }
        None => Err(err),
    }
}

/// `strtol(s, &endp, 10)` for the int2vectorin parser: parse an optional
/// `+`/`-` sign followed by decimal digits, returning the parsed value (as `i64`
/// to allow ERANGE-style overflow detection at the smallint check) and the
/// number of input bytes consumed (0 if no digits, i.e. `intString == endp`).
/// Saturates at `i64` bounds on overflow, which the smallint range check then
/// rejects (mirroring `errno == ERANGE`).
fn strtol_base10(s: &str) -> (i64, usize) {
    let bytes = s.as_bytes();
    let mut i = 0;
    let neg = match bytes.first() {
        Some(b'+') => {
            i = 1;
            false
        }
        Some(b'-') => {
            i = 1;
            true
        }
        _ => false,
    };
    let digit_start = i;
    let mut acc: i64 = 0;
    let mut overflow = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        let d = (bytes[i] - b'0') as i64;
        acc = acc
            .checked_mul(10)
            .and_then(|v| v.checked_add(d))
            .unwrap_or_else(|| {
                overflow = true;
                i64::MAX
            });
        i += 1;
    }
    // No digits consumed -> strtol leaves endp at the start (intString == endp).
    if i == digit_start {
        return (0, 0);
    }
    let val = if overflow {
        if neg {
            i64::MIN
        } else {
            i64::MAX
        }
    } else if neg {
        -acc
    } else {
        acc
    };
    (val, i)
}

/// `int2vectorout` (int.c:224): render an `int2vector` image as a
/// space-separated decimal smallint list. The header is validated first
/// (`check_valid_int2vector`). The caller decodes the header fields and element
/// values off the array image.
pub fn int2vectorout(
    ndim: i32,
    dataoffset: i32,
    elemtype: types_core::Oid,
    values: &[i16],
) -> PgResult<String> {
    check_valid_int2vector(ndim, dataoffset, elemtype)?;
    let mut out = String::new();
    for (i, v) in values.iter().enumerate() {
        if i != 0 {
            out.push(' ');
        }
        out.push_str(&itoa_string(*v));
    }
    Ok(out)
}

// ===========================================================================
// USER I/O ROUTINES (int4)
// ===========================================================================

/// `int4in()` (int.c:315): cstring -> int32 via `pg_strtoint32_safe`.
pub fn int4in(num: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i32> {
    pg_strtoint32_safe(num, escontext)
}

/// `int4out()` (int.c:326): int32 -> "num" (`pg_ltoa`).
pub fn int4out(arg1: i32) -> String {
    ltoa_string(arg1)
}

/// `int4recv()` (int.c:339): `(int32) pq_getmsgint(buf, sizeof(int32))`.
pub fn int4recv(buf: &mut StringInfo<'_>) -> PgResult<i32> {
    Ok(pq_getmsgint(buf, core::mem::size_of::<i32>() as i32)? as i32)
}

/// `int4send()` (int.c:350): `pq_sendint32(&buf, arg1)`.
pub fn int4send<'mcx>(mcx: Mcx<'mcx>, arg1: i32) -> PgResult<Bytea<'mcx>> {
    let mut buf = pq_begintypsend(mcx)?;
    pq_sendint32(&mut buf, arg1 as u32)?;
    Ok(pq_endtypsend(buf))
}

// ===========================================================================
// CONVERSION ROUTINES
// ===========================================================================

/// `i2toi4()` (int.c:368): int16 -> int32 (widen).
pub fn i2toi4(arg1: i16) -> i32 {
    arg1 as i32
}

/// `i4toi2()` (int.c:376): int32 -> int16 (range-checked).
pub fn i4toi2(arg1: i32) -> PgResult<i16> {
    if arg1 < SHRT_MIN || arg1 > SHRT_MAX {
        return Err(smallint_out_of_range());
    }
    Ok(arg1 as i16)
}

/// `int4_bool()` (int.c:390): int4 -> bool (nonzero is true).
pub fn int4_bool(arg: i32) -> bool {
    arg != 0
}

/// `bool_int4()` (int.c:400): bool -> int4 (0 or 1).
pub fn bool_int4(arg: bool) -> i32 {
    if !arg {
        0
    } else {
        1
    }
}

// ===========================================================================
// COMPARISON OPERATOR ROUTINES
// ===========================================================================

/// `int4eq()` (int.c:424).
pub fn int4eq(arg1: i32, arg2: i32) -> bool {
    arg1 == arg2
}
/// `int4ne()` (int.c:433).
pub fn int4ne(arg1: i32, arg2: i32) -> bool {
    arg1 != arg2
}
/// `int4lt()` (int.c:442).
pub fn int4lt(arg1: i32, arg2: i32) -> bool {
    arg1 < arg2
}
/// `int4le()` (int.c:451).
pub fn int4le(arg1: i32, arg2: i32) -> bool {
    arg1 <= arg2
}
/// `int4gt()` (int.c:460).
pub fn int4gt(arg1: i32, arg2: i32) -> bool {
    arg1 > arg2
}
/// `int4ge()` (int.c:469).
pub fn int4ge(arg1: i32, arg2: i32) -> bool {
    arg1 >= arg2
}

/// `int2eq()` (int.c:478).
pub fn int2eq(arg1: i16, arg2: i16) -> bool {
    arg1 == arg2
}
/// `int2ne()` (int.c:487).
pub fn int2ne(arg1: i16, arg2: i16) -> bool {
    arg1 != arg2
}
/// `int2lt()` (int.c:496).
pub fn int2lt(arg1: i16, arg2: i16) -> bool {
    arg1 < arg2
}
/// `int2le()` (int.c:505).
pub fn int2le(arg1: i16, arg2: i16) -> bool {
    arg1 <= arg2
}
/// `int2gt()` (int.c:514).
pub fn int2gt(arg1: i16, arg2: i16) -> bool {
    arg1 > arg2
}
/// `int2ge()` (int.c:523).
pub fn int2ge(arg1: i16, arg2: i16) -> bool {
    arg1 >= arg2
}

/// `int24eq()` (int.c:532): int16 vs int32 (widen lhs).
pub fn int24eq(arg1: i16, arg2: i32) -> bool {
    arg1 as i32 == arg2
}
/// `int24ne()` (int.c:541).
pub fn int24ne(arg1: i16, arg2: i32) -> bool {
    arg1 as i32 != arg2
}
/// `int24lt()` (int.c:550).
pub fn int24lt(arg1: i16, arg2: i32) -> bool {
    (arg1 as i32) < arg2
}
/// `int24le()` (int.c:559).
pub fn int24le(arg1: i16, arg2: i32) -> bool {
    arg1 as i32 <= arg2
}
/// `int24gt()` (int.c:568).
pub fn int24gt(arg1: i16, arg2: i32) -> bool {
    arg1 as i32 > arg2
}
/// `int24ge()` (int.c:577).
pub fn int24ge(arg1: i16, arg2: i32) -> bool {
    arg1 as i32 >= arg2
}

/// `int42eq()` (int.c:586): int32 vs int16 (widen rhs).
pub fn int42eq(arg1: i32, arg2: i16) -> bool {
    arg1 == arg2 as i32
}
/// `int42ne()` (int.c:595).
pub fn int42ne(arg1: i32, arg2: i16) -> bool {
    arg1 != arg2 as i32
}
/// `int42lt()` (int.c:604).
pub fn int42lt(arg1: i32, arg2: i16) -> bool {
    arg1 < arg2 as i32
}
/// `int42le()` (int.c:613).
pub fn int42le(arg1: i32, arg2: i16) -> bool {
    arg1 <= arg2 as i32
}
/// `int42gt()` (int.c:622).
pub fn int42gt(arg1: i32, arg2: i16) -> bool {
    arg1 > arg2 as i32
}
/// `int42ge()` (int.c:631).
pub fn int42ge(arg1: i32, arg2: i16) -> bool {
    arg1 >= arg2 as i32
}

// ===========================================================================
// in_range functions (int.c:651-789)
// ===========================================================================

/// `in_range_int4_int4()` (int.c:651).
pub fn in_range_int4_int4(
    val: i32,
    base: i32,
    mut offset: i32,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    if offset < 0 {
        return Err(invalid_preceding_following());
    }
    if sub {
        offset = -offset; // cannot overflow
    }
    let mut sum = 0i32;
    if add_s32(base, offset, &mut sum) {
        return Ok(if sub { !less } else { less });
    }
    Ok(if less { val <= sum } else { val >= sum })
}

/// `in_range_int4_int2()` (int.c:686): widen the int2 offset and reuse int4_int4.
pub fn in_range_int4_int2(
    val: i32,
    base: i32,
    offset: i16,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    in_range_int4_int4(val, base, offset as i32, sub, less)
}

/// `in_range_int4_int8()` (int.c:697): math in int64.
pub fn in_range_int4_int8(
    val: i32,
    base: i32,
    mut offset: i64,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    let val = val as i64;
    let base = base as i64;
    if offset < 0 {
        return Err(invalid_preceding_following());
    }
    if sub {
        offset = -offset;
    }
    let mut sum = 0i64;
    if add_s64(base, offset, &mut sum) {
        return Ok(if sub { !less } else { less });
    }
    Ok(if less { val <= sum } else { val >= sum })
}

/// `in_range_int2_int4()` (int.c:732): math in int32.
pub fn in_range_int2_int4(
    val: i16,
    base: i16,
    mut offset: i32,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    let val = val as i32;
    let base = base as i32;
    if offset < 0 {
        return Err(invalid_preceding_following());
    }
    if sub {
        offset = -offset;
    }
    let mut sum = 0i32;
    if add_s32(base, offset, &mut sum) {
        return Ok(if sub { !less } else { less });
    }
    Ok(if less { val <= sum } else { val >= sum })
}

/// `in_range_int2_int2()` (int.c:767): widen offset, reuse int2_int4.
pub fn in_range_int2_int2(
    val: i16,
    base: i16,
    offset: i16,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    in_range_int2_int4(val, base, offset as i32, sub, less)
}

/// `in_range_int2_int8()` (int.c:779): widen val/base to int4, reuse int4_int8.
pub fn in_range_int2_int8(
    val: i16,
    base: i16,
    offset: i64,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    in_range_int4_int8(val as i32, base as i32, offset, sub, less)
}

// ===========================================================================
// Arithmetic operators
// ===========================================================================

/// `int4um()` (int.c:800): unary minus, range-checked.
pub fn int4um(arg: i32) -> PgResult<i32> {
    if arg == PG_INT32_MIN {
        return Err(integer_out_of_range());
    }
    Ok(-arg)
}

/// `int4up()` (int.c:811): unary plus.
pub fn int4up(arg: i32) -> i32 {
    arg
}

/// `int4pl()` (int.c:819).
pub fn int4pl(arg1: i32, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_add_s32_overflow(arg1, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int4mi()` (int.c:833).
pub fn int4mi(arg1: i32, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_sub_s32_overflow(arg1, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int4mul()` (int.c:847).
pub fn int4mul(arg1: i32, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_mul_s32_overflow(arg1, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int4div()` (int.c:861).
pub fn int4div(arg1: i32, arg2: i32) -> PgResult<i32> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // Dodge INT_MIN / -1 by recognizing division by -1 == negation.
    if arg2 == -1 {
        if arg1 == PG_INT32_MIN {
            return Err(integer_out_of_range());
        }
        return Ok(-arg1);
    }
    Ok(arg1 / arg2)
}

/// `int4inc()` (int.c:900).
pub fn int4inc(arg: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_add_s32_overflow(arg, 1, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int2um()` (int.c:914).
pub fn int2um(arg: i16) -> PgResult<i16> {
    if arg == PG_INT16_MIN {
        return Err(smallint_out_of_range());
    }
    Ok(-arg)
}

/// `int2up()` (int.c:926).
pub fn int2up(arg: i16) -> i16 {
    arg
}

/// `int2pl()` (int.c:934).
pub fn int2pl(arg1: i16, arg2: i16) -> PgResult<i16> {
    let mut result = 0;
    if pg_add_s16_overflow(arg1, arg2, &mut result) {
        return Err(smallint_out_of_range());
    }
    Ok(result)
}

/// `int2mi()` (int.c:948).
pub fn int2mi(arg1: i16, arg2: i16) -> PgResult<i16> {
    let mut result = 0;
    if pg_sub_s16_overflow(arg1, arg2, &mut result) {
        return Err(smallint_out_of_range());
    }
    Ok(result)
}

/// `int2mul()` (int.c:962).
pub fn int2mul(arg1: i16, arg2: i16) -> PgResult<i16> {
    let mut result = 0;
    if pg_mul_s16_overflow(arg1, arg2, &mut result) {
        return Err(smallint_out_of_range());
    }
    Ok(result)
}

/// `int2div()` (int.c:977).
pub fn int2div(arg1: i16, arg2: i16) -> PgResult<i16> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    if arg2 == -1 {
        if arg1 == PG_INT16_MIN {
            return Err(smallint_out_of_range());
        }
        return Ok(-arg1);
    }
    Ok(arg1 / arg2)
}

/// `int24pl()` (int.c:1016).
pub fn int24pl(arg1: i16, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_add_s32_overflow(arg1 as i32, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int24mi()` (int.c:1030).
pub fn int24mi(arg1: i16, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_sub_s32_overflow(arg1 as i32, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int24mul()` (int.c:1044).
pub fn int24mul(arg1: i16, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_mul_s32_overflow(arg1 as i32, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int24div()` (int.c:1058): no overflow possible; only divide-by-zero.
pub fn int24div(arg1: i16, arg2: i32) -> PgResult<i32> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    Ok(arg1 as i32 / arg2)
}

/// `int42pl()` (int.c:1077).
pub fn int42pl(arg1: i32, arg2: i16) -> PgResult<i32> {
    let mut result = 0;
    if pg_add_s32_overflow(arg1, arg2 as i32, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int42mi()` (int.c:1091).
pub fn int42mi(arg1: i32, arg2: i16) -> PgResult<i32> {
    let mut result = 0;
    if pg_sub_s32_overflow(arg1, arg2 as i32, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int42mul()` (int.c:1105).
pub fn int42mul(arg1: i32, arg2: i16) -> PgResult<i32> {
    let mut result = 0;
    if pg_mul_s32_overflow(arg1, arg2 as i32, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

/// `int42div()` (int.c:1119).
pub fn int42div(arg1: i32, arg2: i16) -> PgResult<i32> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    if arg2 == -1 {
        if arg1 == PG_INT32_MIN {
            return Err(integer_out_of_range());
        }
        return Ok(-arg1);
    }
    Ok(arg1 / arg2 as i32)
}

/// `int4mod()` (int.c:1158).
pub fn int4mod(arg1: i32, arg2: i32) -> PgResult<i32> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // INT_MIN % -1 is well-defined zero; dodge the FPE.
    if arg2 == -1 {
        return Ok(0);
    }
    Ok(arg1 % arg2)
}

/// `int2mod()` (int.c:1186).
pub fn int2mod(arg1: i16, arg2: i16) -> PgResult<i16> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    if arg2 == -1 {
        return Ok(0);
    }
    Ok(arg1 % arg2)
}

/// `int4abs()` (int.c:1219).
pub fn int4abs(arg1: i32) -> PgResult<i32> {
    if arg1 == PG_INT32_MIN {
        return Err(integer_out_of_range());
    }
    Ok(if arg1 < 0 { -arg1 } else { arg1 })
}

/// `int2abs()` (int.c:1233).
pub fn int2abs(arg1: i16) -> PgResult<i16> {
    if arg1 == PG_INT16_MIN {
        return Err(smallint_out_of_range());
    }
    Ok(if arg1 < 0 { -arg1 } else { arg1 })
}

/// `int4gcd_internal()` (int.c:1261): GCD with INT_MIN special-casing.
pub fn int4gcd_internal(mut arg1: i32, mut arg2: i32) -> PgResult<i32> {
    // Put the greater absolute value in arg1 (worked in negative space to
    // handle INT_MIN).
    let a1 = if arg1 < 0 { arg1 } else { -arg1 };
    let a2 = if arg2 < 0 { arg2 } else { -arg2 };
    if a1 > a2 {
        core::mem::swap(&mut arg1, &mut arg2);
    }

    if arg1 == PG_INT32_MIN {
        if arg2 == 0 || arg2 == PG_INT32_MIN {
            return Err(integer_out_of_range());
        }
        if arg2 == -1 {
            return Ok(1);
        }
    }

    // Euclidean algorithm.
    while arg2 != 0 {
        let swap = arg2;
        arg2 = arg1 % arg2;
        arg1 = swap;
    }

    if arg1 < 0 {
        arg1 = -arg1;
    }
    Ok(arg1)
}

/// `int4gcd()` (int.c:1322).
pub fn int4gcd(arg1: i32, arg2: i32) -> PgResult<i32> {
    int4gcd_internal(arg1, arg2)
}

/// `int4lcm()` (int.c:1337).
pub fn int4lcm(arg1: i32, arg2: i32) -> PgResult<i32> {
    if arg1 == 0 || arg2 == 0 {
        return Ok(0);
    }
    let gcd = int4gcd_internal(arg1, arg2)?;
    let arg1 = arg1 / gcd;
    let mut result = 0;
    if pg_mul_s32_overflow(arg1, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    if result == PG_INT32_MIN {
        return Err(integer_out_of_range());
    }
    if result < 0 {
        result = -result;
    }
    Ok(result)
}

/// `int2larger()` (int.c:1374).
pub fn int2larger(arg1: i16, arg2: i16) -> i16 {
    if arg1 > arg2 {
        arg1
    } else {
        arg2
    }
}

/// `int2smaller()` (int.c:1383).
pub fn int2smaller(arg1: i16, arg2: i16) -> i16 {
    if arg1 < arg2 {
        arg1
    } else {
        arg2
    }
}

/// `int4larger()` (int.c:1392).
pub fn int4larger(arg1: i32, arg2: i32) -> i32 {
    if arg1 > arg2 {
        arg1
    } else {
        arg2
    }
}

/// `int4smaller()` (int.c:1401).
pub fn int4smaller(arg1: i32, arg2: i32) -> i32 {
    if arg1 < arg2 {
        arg1
    } else {
        arg2
    }
}

// ===========================================================================
// Bit-pushing operators
// ===========================================================================

/// `int4and()` (int.c:1421).
pub fn int4and(arg1: i32, arg2: i32) -> i32 {
    arg1 & arg2
}
/// `int4or()` (int.c:1430).
pub fn int4or(arg1: i32, arg2: i32) -> i32 {
    arg1 | arg2
}
/// `int4xor()` (int.c:1439).
pub fn int4xor(arg1: i32, arg2: i32) -> i32 {
    arg1 ^ arg2
}
/// `int4shl()` (int.c:1448): C `arg1 << arg2`.
pub fn int4shl(arg1: i32, arg2: i32) -> i32 {
    arg1.wrapping_shl(arg2 as u32)
}
/// `int4shr()` (int.c:1457): C arithmetic `arg1 >> arg2`.
pub fn int4shr(arg1: i32, arg2: i32) -> i32 {
    arg1.wrapping_shr(arg2 as u32)
}
/// `int4not()` (int.c:1466).
pub fn int4not(arg1: i32) -> i32 {
    !arg1
}

/// `int2and()` (int.c:1474).
pub fn int2and(arg1: i16, arg2: i16) -> i16 {
    arg1 & arg2
}
/// `int2or()` (int.c:1483).
pub fn int2or(arg1: i16, arg2: i16) -> i16 {
    arg1 | arg2
}
/// `int2xor()` (int.c:1492).
pub fn int2xor(arg1: i16, arg2: i16) -> i16 {
    arg1 ^ arg2
}
/// `int2not()` (int.c:1501).
pub fn int2not(arg1: i16) -> i16 {
    !arg1
}
/// `int2shl()` (int.c:1510): `(int16)(arg1 << arg2)` with an int32 shift amount.
pub fn int2shl(arg1: i16, arg2: i32) -> i16 {
    ((arg1 as i32).wrapping_shl(arg2 as u32)) as i16
}
/// `int2shr()` (int.c:1519): `(int16)(arg1 >> arg2)`.
pub fn int2shr(arg1: i16, arg2: i32) -> i16 {
    ((arg1 as i32).wrapping_shr(arg2 as u32)) as i16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_overflow_messages() {
        let err = int4pl(i32::MAX, 1).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(err.message(), "integer out of range");

        let err = int2mul(i16::MAX, 2).unwrap_err();
        assert_eq!(err.message(), "smallint out of range");

        let err = int4div(1, 0).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_DIVISION_BY_ZERO);
        assert_eq!(err.message(), "division by zero");

        // INT_MIN / -1 dodged as negation -> overflow error.
        let err = int4div(i32::MIN, -1).unwrap_err();
        assert_eq!(err.message(), "integer out of range");
        // INT_MIN % -1 == 0 (no error).
        assert_eq!(int4mod(i32::MIN, -1).unwrap(), 0);
    }

    #[test]
    fn conversions_and_casts() {
        assert_eq!(i2toi4(-5), -5);
        assert_eq!(i4toi2(100).unwrap(), 100);
        assert!(i4toi2(40000).is_err());
        assert!(!int4_bool(0));
        assert!(int4_bool(7));
        assert_eq!(bool_int4(true), 1);
        assert_eq!(bool_int4(false), 0);
    }

    #[test]
    fn out_renders_decimal() {
        assert_eq!(int2out(-12345), "-12345");
        assert_eq!(int2out(0), "0");
        assert_eq!(int4out(2147483647), "2147483647");
        assert_eq!(int4out(i32::MIN), "-2147483648");
    }

    #[test]
    fn in_parses_decimal() {
        assert_eq!(int2in("123", None).unwrap(), 123);
        assert_eq!(int4in("-7", None).unwrap(), -7);
        assert!(int2in("99999", None).is_err());
    }

    #[test]
    fn gcd_lcm_and_abs() {
        assert_eq!(int4gcd(12, 8).unwrap(), 4);
        assert_eq!(int4gcd(0, 0).unwrap(), 0);
        assert_eq!(int4gcd(i32::MIN, -1).unwrap(), 1);
        assert!(int4gcd(i32::MIN, 0).is_err());
        assert_eq!(int4lcm(4, 6).unwrap(), 12);
        assert_eq!(int4lcm(0, 5).unwrap(), 0);
        assert_eq!(int4abs(-7).unwrap(), 7);
        assert!(int4abs(i32::MIN).is_err());
        assert_eq!(int2abs(-7).unwrap(), 7);
        assert!(int2abs(i16::MIN).is_err());
    }

    // Exact reproduction of the int4.sql gcd()/lcm() regression rows, including
    // the `-b` operand (computed via int4um, as the SQL `lcm(a,-b)` does). This
    // pins the helpers to PostgreSQL's expected output and guards the wasm64
    // INT_MIN-negation correctness fix. Each tuple is (a, b, expected for the
    // four column expressions: f(a,b), f(a,-b), f(b,a), f(-b,a)).
    fn check_gcd_row(a: i32, b: i32, exp: [i32; 4]) {
        let nb = int4um(b).unwrap();
        assert_eq!(int4gcd(a, b).unwrap(), exp[0], "gcd({a},{b})");
        assert_eq!(int4gcd(a, nb).unwrap(), exp[1], "gcd({a},-{b})");
        assert_eq!(int4gcd(b, a).unwrap(), exp[2], "gcd({b},{a})");
        assert_eq!(int4gcd(nb, a).unwrap(), exp[3], "gcd(-{b},{a})");
    }

    fn check_lcm_row(a: i32, b: i32, exp: [i32; 4]) {
        let nb = int4um(b).unwrap();
        assert_eq!(int4lcm(a, b).unwrap(), exp[0], "lcm({a},{b})");
        assert_eq!(int4lcm(a, nb).unwrap(), exp[1], "lcm({a},-{b})");
        assert_eq!(int4lcm(b, a).unwrap(), exp[2], "lcm({b},{a})");
        assert_eq!(int4lcm(nb, a).unwrap(), exp[3], "lcm(-{b},{a})");
    }

    #[test]
    fn gcd_lcm_int4sql_rows() {
        // gcd() rows from int4.sql.
        check_gcd_row(0, 0, [0, 0, 0, 0]);
        check_gcd_row(0, 6410818, [6410818, 6410818, 6410818, 6410818]);
        check_gcd_row(61866666, 6410818, [1466, 1466, 1466, 1466]);
        check_gcd_row(-61866666, 6410818, [1466, 1466, 1466, 1466]);
        check_gcd_row(i32::MIN, 1, [1, 1, 1, 1]);
        check_gcd_row(i32::MIN, 2147483647, [1, 1, 1, 1]);
        check_gcd_row(i32::MIN, 1073741824, [1073741824, 1073741824, 1073741824, 1073741824]);

        // lcm() rows from int4.sql. The last row is the one that errors on wasm64.
        check_lcm_row(0, 0, [0, 0, 0, 0]);
        check_lcm_row(0, 42, [0, 0, 0, 0]);
        check_lcm_row(42, 42, [42, 42, 42, 42]);
        check_lcm_row(330, 462, [2310, 2310, 2310, 2310]);
        check_lcm_row(-330, 462, [2310, 2310, 2310, 2310]);
        check_lcm_row(i32::MIN, 0, [0, 0, 0, 0]);
    }

    #[test]
    fn in_range_overflow_branches() {
        assert!(in_range_int4_int4(5, i32::MAX, 10, false, true).unwrap());
        assert!(!in_range_int4_int4(5, i32::MAX, 10, false, false).unwrap());
        let err = in_range_int4_int4(0, 0, -1, false, true).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE);
        assert_eq!(
            err.message(),
            "invalid preceding or following size in window function"
        );
        assert!(in_range_int4_int4(5, 3, 4, false, true).unwrap()); // 5 <= 7
        assert!(!in_range_int4_int4(8, 3, 4, false, true).unwrap()); // 8 <= 7 false
        assert!(in_range_int4_int8(5, 3, 4, false, true).unwrap());
        assert!(in_range_int2_int8(2, 1, 3, false, true).unwrap());
    }

    #[test]
    fn bit_ops() {
        assert_eq!(int4and(0b1100, 0b1010), 0b1000);
        assert_eq!(int4or(0b1100, 0b1010), 0b1110);
        assert_eq!(int4xor(0b1100, 0b1010), 0b0110);
        assert_eq!(int4not(0), -1);
        assert_eq!(int4shl(1, 4), 16);
        assert_eq!(int4shr(-16, 2), -4);
        assert_eq!(int2shl(1, 4), 16);
        assert_eq!(int2not(0), -1);
    }

    #[test]
    fn larger_smaller_and_inc() {
        assert_eq!(int2larger(3, 9), 9);
        assert_eq!(int2smaller(3, 9), 3);
        assert_eq!(int4larger(3, 9), 9);
        assert_eq!(int4smaller(3, 9), 3);
        assert_eq!(int4inc(41).unwrap(), 42);
        assert!(int4inc(i32::MAX).is_err());
        assert_eq!(int4um(5).unwrap(), -5);
        assert!(int4um(i32::MIN).is_err());
        assert_eq!(int2um(5).unwrap(), -5);
        assert!(int2um(i16::MIN).is_err());
    }
}
