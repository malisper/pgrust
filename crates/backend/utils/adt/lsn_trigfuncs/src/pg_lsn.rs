//! `src/backend/utils/adt/pg_lsn.c` (postgres-18.3) — operations for the
//! `pg_lsn` datatype.
//!
//! The on-disk / wire representation of a `pg_lsn` value is an
//! [`XLogRecPtr`](::types_core::XLogRecPtr) (a 64-bit unsigned integer); the C
//! code stores it as an `int8` `Datum` and renders it as `"%X/%X"` text. Every
//! SQL-callable function `pg_lsn.c` owns is ported here with the original C
//! name and logic / branch-order / message-text / SQLSTATE preserved 1:1.
//!
//! The arithmetic operators (`pg_lsn_mi` / `pg_lsn_pli` / `pg_lsn_mii`) bridge
//! into the ported numeric crate exactly as the C does via `DirectFunctionCall`
//! into `numeric_in` / `numeric_add` / `numeric_sub` / `numeric_pg_lsn`.
//! `numeric_pg_lsn` is a `numeric.c` function the numeric crate does not expose;
//! it is reproduced here 1:1 over that crate's public API (`set_var_from_num`,
//! `numericvar_to_uint64`).

use mcx::{Mcx, PgVec};
use ::types_core::XLogRecPtr;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
};
use ::types_numeric::var::NumericSign;

use ::adt_numeric::convert::{numericvar_to_uint64, set_var_from_num};
use ::adt_numeric::io::numeric_in;
use ::adt_numeric::ops_sql::{numeric_add, numeric_sub};
use hashfn::{hash_bytes_uint32, hash_bytes_uint32_extended};

/// `#define MAXPG_LSNLEN 17`
pub const MAXPG_LSNLEN: usize = 17;
/// `#define MAXPG_LSNCOMPONENT 8`
pub const MAXPG_LSNCOMPONENT: usize = 8;

/// `InvalidXLogRecPtr` (== 0).
pub const INVALID_XLOG_REC_PTR: XLogRecPtr = ::types_core::InvalidXLogRecPtr;

// ---------------------------------------------------------------------------
// Formatting and conversion routines
// ---------------------------------------------------------------------------

/// True for a character accepted by `strspn(str, "0123456789abcdefABCDEF")`.
#[inline]
fn is_hex_digit(c: u8) -> bool {
    c.is_ascii_digit() || (b'a'..=b'f').contains(&c) || (b'A'..=b'F').contains(&c)
}

/// `pg_lsn_in_internal()` (pg_lsn.c:28) — parse `"%X/%X"` LSN text into an
/// `XLogRecPtr`.
///
/// Mirrors the C control flow exactly: it `strspn`s the first hex run (1..=8
/// chars), requires a `'/'` delimiter, `strspn`s the second hex run (1..=8
/// chars), requires a NUL terminator, then decodes each run with base-16
/// `strtoul` and combines as `(id << 32) | off`.
///
/// On any format violation it sets the `have_error` flag (the second tuple
/// element) and returns `InvalidXLogRecPtr` (0), exactly like the C
/// `*have_error = true` path. Returns `(result, have_error)`.
pub fn pg_lsn_in_internal(str: &str) -> (XLogRecPtr, bool) {
    let bytes = str.as_bytes();

    // Sanity check input format.
    // len1 = strspn(str, hexdigits)
    // C: `len1 < 1 || len1 > MAXPG_LSNCOMPONENT || str[len1] != '/'`.
    let len1 = bytes.iter().take_while(|&&c| is_hex_digit(c)).count();
    if !(1..=MAXPG_LSNCOMPONENT).contains(&len1) || bytes.get(len1) != Some(&b'/') {
        return (INVALID_XLOG_REC_PTR, true);
    }
    // len2 = strspn(str + len1 + 1, hexdigits)
    // C: `len2 < 1 || len2 > MAXPG_LSNCOMPONENT || str[len1 + 1 + len2] != '\0'`
    // — the trailing-NUL check means there must be no trailing junk.
    let len2 = bytes[len1 + 1..]
        .iter()
        .take_while(|&&c| is_hex_digit(c))
        .count();
    if !(1..=MAXPG_LSNCOMPONENT).contains(&len2) || bytes.get(len1 + 1 + len2).is_some() {
        return (INVALID_XLOG_REC_PTR, true);
    }

    // Decode result. Both runs are 1..=8 hex digits, so they fit a u32.
    // strtoul(str, NULL, 16) reads the leading hex run (stopping at '/').
    let id = u32::from_str_radix(&str[..len1], 16).expect("validated 1..=8 hex digits");
    let off = u32::from_str_radix(&str[len1 + 1..len1 + 1 + len2], 16)
        .expect("validated 1..=8 hex digits");
    let result = ((id as u64) << 32) | off as u64;

    (result, false)
}

/// `pg_lsn_in()` (pg_lsn.c:62) — text input function.
///
/// Routes a parse failure through [`ereturn`] with
/// `ERRCODE_INVALID_TEXT_REPRESENTATION`, message
/// `invalid input syntax for type pg_lsn: "<str>"` (pg_lsn.c uses
/// `ereturn(fcinfo->context, ...)`). With a soft `escontext` this records the
/// error and returns `Ok(InvalidXLogRecPtr)`; otherwise it returns `Err`.
pub fn pg_lsn_in(str: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<XLogRecPtr> {
    let (result, have_error) = pg_lsn_in_internal(str);
    if have_error {
        return ereturn(
            escontext,
            INVALID_XLOG_REC_PTR,
            PgError::error(format!(
                "invalid input syntax for type {}: \"{}\"",
                "pg_lsn", str
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        );
    }
    Ok(result)
}

/// `pg_lsn_out()` (pg_lsn.c:79) — text output, `snprintf(buf, "%X/%X", ...)`.
///
/// `LSN_FORMAT_ARGS(lsn)` expands to `(uint32)(lsn >> 32)`, `(uint32) lsn`, and
/// `%X` is uppercase hex with no zero padding. Returns the owned display string
/// (the `pstrdup`'d `cstring` result of the C function).
pub fn pg_lsn_out(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// `pg_lsn_recv()` (pg_lsn.c:91) — binary receive (`pq_getmsgint64`).
///
/// `pq_getmsgint64` reads a network-order (big-endian) `int64` off the message
/// buffer; the value is reinterpreted as the `pg_lsn`/`XLogRecPtr`. A short
/// buffer mirrors C's `pq_getmsgint64` running off the message end
/// (`ERRCODE_PROTOCOL_VIOLATION`), reported here as an error.
pub fn pg_lsn_recv(buf: &[u8]) -> PgResult<XLogRecPtr> {
    if buf.len() < 8 {
        return Err(PgError::error(
            "insufficient data left in message",
        )
        .with_sqlstate(::types_error::ERRCODE_PROTOCOL_VIOLATION));
    }
    let bytes: [u8; 8] = buf[..8].try_into().expect("checked len >= 8");
    Ok(u64::from_be_bytes(bytes))
}

/// `pg_lsn_send()` (pg_lsn.c:101) — binary send (`pq_sendint64`). Returns the
/// owned `bytea` payload bytes (the network-order int64), allocated in `mcx`
/// (the C `pq_begintypsend`/`pq_endtypsend` palloc'd buffer).
pub fn pg_lsn_send<'mcx>(mcx: Mcx<'mcx>, lsn: XLogRecPtr) -> PgResult<PgVec<'mcx, u8>> {
    let mut buf = PgVec::new_in(mcx);
    buf.try_reserve(8).map_err(|_| mcx.oom(8))?;
    buf.extend_from_slice(&lsn.to_be_bytes());
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Operators for PostgreSQL LSNs
// ---------------------------------------------------------------------------

/// `pg_lsn_eq()` (pg_lsn.c:117).
pub fn pg_lsn_eq(lsn1: XLogRecPtr, lsn2: XLogRecPtr) -> bool {
    lsn1 == lsn2
}

/// `pg_lsn_ne()` (pg_lsn.c:126).
pub fn pg_lsn_ne(lsn1: XLogRecPtr, lsn2: XLogRecPtr) -> bool {
    lsn1 != lsn2
}

/// `pg_lsn_lt()` (pg_lsn.c:135).
pub fn pg_lsn_lt(lsn1: XLogRecPtr, lsn2: XLogRecPtr) -> bool {
    lsn1 < lsn2
}

/// `pg_lsn_gt()` (pg_lsn.c:144).
pub fn pg_lsn_gt(lsn1: XLogRecPtr, lsn2: XLogRecPtr) -> bool {
    lsn1 > lsn2
}

/// `pg_lsn_le()` (pg_lsn.c:153).
pub fn pg_lsn_le(lsn1: XLogRecPtr, lsn2: XLogRecPtr) -> bool {
    lsn1 <= lsn2
}

/// `pg_lsn_ge()` (pg_lsn.c:162).
pub fn pg_lsn_ge(lsn1: XLogRecPtr, lsn2: XLogRecPtr) -> bool {
    lsn1 >= lsn2
}

/// `pg_lsn_larger()` (pg_lsn.c:171) — returns the greater of two LSNs.
pub fn pg_lsn_larger(lsn1: XLogRecPtr, lsn2: XLogRecPtr) -> XLogRecPtr {
    if lsn1 > lsn2 {
        lsn1
    } else {
        lsn2
    }
}

/// `pg_lsn_smaller()` (pg_lsn.c:180) — returns the lesser of two LSNs.
pub fn pg_lsn_smaller(lsn1: XLogRecPtr, lsn2: XLogRecPtr) -> XLogRecPtr {
    if lsn1 < lsn2 {
        lsn1
    } else {
        lsn2
    }
}

/// `pg_lsn_cmp()` (pg_lsn.c:190) — btree comparator (1 / 0 / -1).
pub fn pg_lsn_cmp(a: XLogRecPtr, b: XLogRecPtr) -> i32 {
    if a > b {
        1
    } else if a == b {
        0
    } else {
        -1
    }
}

/// `pg_lsn_hash()` (pg_lsn.c:205) — `return hashint8(fcinfo);`.
///
/// The `pg_lsn` value is the `int8`/`XLogRecPtr` reinterpreted as a signed
/// `int64`, fed straight into [`hashint8`].
pub fn pg_lsn_hash(lsn: XLogRecPtr) -> u32 {
    hashint8(lsn as i64)
}

/// `pg_lsn_hash_extended()` (pg_lsn.c:212) — `return hashint8extended(fcinfo);`.
pub fn pg_lsn_hash_extended(lsn: XLogRecPtr, seed: u64) -> u64 {
    hashint8extended(lsn as i64, seed)
}

/// `hashint8()` (`access/hash/hashfunc.c`) — the int8 hash fold that
/// `pg_lsn_hash` delegates to.
///
/// `hashfunc.c` is not yet a crate of its own; this is a faithful copy of its
/// sign-dependent fold delegating the final mix to the ported
/// [`::hashfn::hash_bytes_uint32`] (== `common/hashfn.c`'s `hash_uint32`).
///
/// ```c
/// int64  val    = PG_GETARG_INT64(0);
/// uint32 lohalf = (uint32) val;
/// uint32 hihalf = (uint32) (val >> 32);
/// lohalf ^= (val >= 0) ? hihalf : ~hihalf;
/// return hash_uint32(lohalf);
/// ```
#[inline]
fn hashint8(val: i64) -> u32 {
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let lohalf = lohalf ^ if val >= 0 { hihalf } else { !hihalf };
    hash_bytes_uint32(lohalf)
}

/// `hashint8extended()` (`access/hash/hashfunc.c`) — seeded int8 hash fold.
#[inline]
fn hashint8extended(val: i64, seed: u64) -> u64 {
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let lohalf = lohalf ^ if val >= 0 { hihalf } else { !hihalf };
    hash_bytes_uint32_extended(lohalf, seed)
}

// ---------------------------------------------------------------------------
// Arithmetic operators
// ---------------------------------------------------------------------------

/// `numeric_pg_lsn()` (numeric.c:4868) — convert a numeric (here the on-disk
/// byte image) to an `XLogRecPtr`.
///
/// Reproduced 1:1 from `numeric.c` because `numeric_pg_lsn` is the function
/// `pg_lsn_pli` / `pg_lsn_mii` call via `DirectFunctionCall1`, and the numeric
/// crate does not expose it:
///
/// * NaN  -> `ERRCODE_FEATURE_NOT_SUPPORTED`, `cannot convert NaN to pg_lsn`.
/// * +-Inf -> `ERRCODE_FEATURE_NOT_SUPPORTED`, `cannot convert infinity to pg_lsn`.
/// * otherwise `numericvar_to_uint64` (round-to-nearest); failure (negative or
///   `> 2^64 - 1`) -> `ERRCODE_INVALID_PARAMETER_VALUE`, `pg_lsn out of range`.
pub fn numeric_pg_lsn(mcx: Mcx<'_>, num: &[u8]) -> PgResult<XLogRecPtr> {
    let x = set_var_from_num(mcx, num)?;

    if x.is_special() {
        if x.sign == NumericSign::NaN {
            return Err(
                PgError::error(format!("cannot convert {} to {}", "NaN", "pg_lsn"))
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            );
        } else {
            return Err(
                PgError::error(format!("cannot convert {} to {}", "infinity", "pg_lsn"))
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            );
        }
    }

    // Convert to variable format and thence to pg_lsn.
    match numericvar_to_uint64(&x)? {
        Some(result) => Ok(result),
        None => Err(PgError::error("pg_lsn out of range")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)),
    }
}

/// `pg_lsn_mi()` (pg_lsn.c:223) — subtract two LSNs, returning a `numeric`
/// (the on-disk varlena byte image, the `DirectFunctionCall3(numeric_in, ...)`
/// result allocated in `mcx`).
///
/// The C code formats the unsigned difference (with a leading `-` when
/// `lsn1 < lsn2`) into a decimal string — the magnitude fits in `2^64 - 1`, well
/// within the 256-byte buffer — and feeds it through `numeric_in`.
pub fn pg_lsn_mi<'mcx>(
    mcx: Mcx<'mcx>,
    lsn1: XLogRecPtr,
    lsn2: XLogRecPtr,
) -> PgResult<PgVec<'mcx, u8>> {
    // Output could be as large as plus or minus 2^63 - 1.
    let buf = if lsn1 < lsn2 {
        format!("-{}", lsn2 - lsn1)
    } else {
        format!("{}", lsn1 - lsn2)
    };

    // Convert to numeric.
    numeric_in(mcx, &buf, -1)
}

/// `pg_lsn_pli()` (pg_lsn.c:250) — add the number of bytes (`nbytes`, an on-disk
/// `Numeric`) to `lsn`, giving a new `pg_lsn`. Handles both positive and
/// negative byte counts.
///
/// NaN is rejected up front (`ERRCODE_FEATURE_NOT_SUPPORTED`,
/// `cannot add NaN to pg_lsn`); otherwise the lsn is rendered as a decimal
/// integer, parsed via `numeric_in`, added to `nbytes`, and converted back to a
/// `pg_lsn` via [`numeric_pg_lsn`].
pub fn pg_lsn_pli(mcx: Mcx<'_>, lsn: XLogRecPtr, nbytes: &[u8]) -> PgResult<XLogRecPtr> {
    if ::types_numeric::numeric_is_nan(nbytes) {
        return Err(PgError::error("cannot add NaN to pg_lsn")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // Convert to numeric.
    let num = numeric_in(mcx, &format!("{lsn}"), -1)?;

    // Add two numerics.
    let res = numeric_add(mcx, &num, nbytes)?;

    // Convert to pg_lsn.
    numeric_pg_lsn(mcx, &res)
}

/// `pg_lsn_mii()` (pg_lsn.c:284) — subtract the number of bytes (`nbytes`, an
/// on-disk `Numeric`) from `lsn`, giving a new `pg_lsn`. Handles both positive
/// and negative byte counts.
///
/// NaN is rejected up front (`ERRCODE_FEATURE_NOT_SUPPORTED`,
/// `cannot subtract NaN from pg_lsn`); otherwise the lsn is rendered as a decimal
/// integer, parsed via `numeric_in`, has `nbytes` subtracted, and is converted
/// back to a `pg_lsn` via [`numeric_pg_lsn`].
pub fn pg_lsn_mii(mcx: Mcx<'_>, lsn: XLogRecPtr, nbytes: &[u8]) -> PgResult<XLogRecPtr> {
    if ::types_numeric::numeric_is_nan(nbytes) {
        return Err(PgError::error("cannot subtract NaN from pg_lsn")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // Convert to numeric.
    let num = numeric_in(mcx, &format!("{lsn}"), -1)?;

    // Subtract two numerics.
    let res = numeric_sub(mcx, &num, nbytes)?;

    // Convert to pg_lsn.
    numeric_pg_lsn(mcx, &res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::adt_numeric::io::numeric_out;
    use ::mcx::MemoryContext;

    /// Build an on-disk Numeric varlena from a decimal/special string.
    fn nbytes<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgVec<'mcx, u8> {
        numeric_in(mcx, s, -1).unwrap()
    }

    #[test]
    fn in_internal_largest_and_smallest() {
        assert_eq!(pg_lsn_in_internal("0/0"), (0, false));
        assert_eq!(
            pg_lsn_in_internal("FFFFFFFF/FFFFFFFF"),
            (0xFFFF_FFFF_FFFF_FFFF, false)
        );
        assert_eq!(pg_lsn_in_internal("0/16AE7F8"), (0x016A_E7F8, false));
    }

    #[test]
    fn in_internal_rejects_bad_format() {
        for bad in ["G/0", "-1/0", " 0/12345678", "ABCD/", "/ABCD", "16AE7F7"] {
            let (result, have_error) = pg_lsn_in_internal(bad);
            assert!(have_error, "expected error for {bad:?}");
            assert_eq!(result, INVALID_XLOG_REC_PTR);
        }
        // 9 hex digits in either component is too long.
        assert!(pg_lsn_in_internal("FFFFFFFFF/0").1);
        assert!(pg_lsn_in_internal("0/FFFFFFFFF").1);
    }

    #[test]
    fn pg_lsn_in_soft_and_hard_errors() {
        let err = pg_lsn_in("16AE7F7", None).unwrap_err();
        assert_eq!(
            err.message(),
            "invalid input syntax for type pg_lsn: \"16AE7F7\""
        );
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

        let mut escontext = SoftErrorContext::new(true);
        let result = pg_lsn_in("16AE7F7", Some(&mut escontext)).unwrap();
        assert_eq!(result, INVALID_XLOG_REC_PTR);
        assert!(escontext.error_occurred());

        assert_eq!(pg_lsn_in("0/16AE7F8", None).unwrap(), 0x016A_E7F8);
    }

    #[test]
    fn out_and_round_trip() {
        assert_eq!(pg_lsn_out(0), "0/0");
        assert_eq!(pg_lsn_out(0xFFFF_FFFF_FFFF_FFFF), "FFFFFFFF/FFFFFFFF");
        for s in ["0/0", "FFFFFFFF/FFFFFFFF", "0/16AE7F8", "1/1", "10/10"] {
            let (lsn, err) = pg_lsn_in_internal(s);
            assert!(!err);
            assert_eq!(pg_lsn_out(lsn), s);
        }
    }

    #[test]
    fn recv_send_round_trip() {
        let ctx = MemoryContext::new("lsn-recv-send");
        let mcx = ctx.mcx();
        let lsn: XLogRecPtr = 0x0123_4567_89AB_CDEF;
        let sent = pg_lsn_send(mcx, lsn).unwrap();
        assert_eq!(&sent[..], &lsn.to_be_bytes());
        assert_eq!(pg_lsn_recv(&sent).unwrap(), lsn);
        // Short buffer is a protocol violation.
        assert!(pg_lsn_recv(&[0u8; 7]).is_err());
    }

    #[test]
    fn comparison_operators() {
        let a: XLogRecPtr = 0x016A_E7F7;
        let b: XLogRecPtr = 0x016A_E7F8;
        assert!(pg_lsn_eq(a, a));
        assert!(pg_lsn_ne(a, b));
        assert!(pg_lsn_lt(a, b));
        assert!(pg_lsn_gt(b, a));
        assert!(pg_lsn_le(a, a));
        assert!(pg_lsn_ge(a, a));
        assert_eq!(pg_lsn_larger(a, b), b);
        assert_eq!(pg_lsn_smaller(a, b), a);
        assert_eq!(pg_lsn_cmp(b, a), 1);
        assert_eq!(pg_lsn_cmp(a, a), 0);
        assert_eq!(pg_lsn_cmp(a, b), -1);
    }

    #[test]
    fn hash_matches_int8_fold() {
        let lsn: XLogRecPtr = 0xFFFF_FFFF_FFFF_FFFF;
        assert_eq!(pg_lsn_hash(lsn), hashint8(lsn as i64));
        assert_eq!(
            pg_lsn_hash_extended(lsn, 42),
            hashint8extended(lsn as i64, 42)
        );
        let neg_lsn: XLogRecPtr = 0x8000_0000_0000_0001;
        assert!((neg_lsn as i64) < 0);
        assert_eq!(pg_lsn_hash(neg_lsn), hashint8(neg_lsn as i64));
    }

    #[test]
    fn mi_subtracts_to_numeric_signed() {
        let ctx = MemoryContext::new("lsn-mi");
        let mcx = ctx.mcx();
        let a = pg_lsn_in_internal("0/16AE7F7").0;
        let b = pg_lsn_in_internal("0/16AE7F8").0;
        assert_eq!(numeric_out(mcx, &pg_lsn_mi(mcx, a, b).unwrap()).unwrap(), "-1");
        assert_eq!(numeric_out(mcx, &pg_lsn_mi(mcx, b, a).unwrap()).unwrap(), "1");
        assert_eq!(numeric_out(mcx, &pg_lsn_mi(mcx, a, a).unwrap()).unwrap(), "0");

        let lo = pg_lsn_in_internal("0/0").0;
        let hi = pg_lsn_in_internal("FFFFFFFF/FFFFFFFF").0;
        assert_eq!(
            numeric_out(mcx, &pg_lsn_mi(mcx, hi, lo).unwrap()).unwrap(),
            "18446744073709551615"
        );
        assert_eq!(
            numeric_out(mcx, &pg_lsn_mi(mcx, lo, hi).unwrap()).unwrap(),
            "-18446744073709551615"
        );
    }

    #[test]
    fn pli_and_mii_arithmetic() {
        let ctx = MemoryContext::new("lsn-pli-mii");
        let mcx = ctx.mcx();
        let base = pg_lsn_in_internal("0/16AE7F7").0;
        assert_eq!(
            pg_lsn_pli(mcx, base, &nbytes(mcx, "16")).unwrap(),
            pg_lsn_in_internal("0/16AE807").0
        );
        assert_eq!(
            pg_lsn_mii(mcx, base, &nbytes(mcx, "16")).unwrap(),
            pg_lsn_in_internal("0/16AE7E7").0
        );
        // Negative nbytes on pli.
        assert_eq!(
            pg_lsn_pli(mcx, base, &nbytes(mcx, "-16")).unwrap(),
            pg_lsn_in_internal("0/16AE7E7").0
        );
        // Boundary: FFFFFFFF/FFFFFFFE + 1 = FFFFFFFF/FFFFFFFF.
        let near_max = pg_lsn_in_internal("FFFFFFFF/FFFFFFFE").0;
        assert_eq!(
            pg_lsn_pli(mcx, near_max, &nbytes(mcx, "1")).unwrap(),
            pg_lsn_in_internal("FFFFFFFF/FFFFFFFF").0
        );
        // 0/1 - 1 = 0/0.
        assert_eq!(pg_lsn_mii(mcx, 1, &nbytes(mcx, "1")).unwrap(), 0);
    }

    #[test]
    fn pli_and_mii_out_of_range() {
        let ctx = MemoryContext::new("lsn-oor");
        let mcx = ctx.mcx();
        let near_max = pg_lsn_in_internal("FFFFFFFF/FFFFFFFE").0;
        let err = pg_lsn_pli(mcx, near_max, &nbytes(mcx, "2")).unwrap_err();
        assert_eq!(err.message(), "pg_lsn out of range");
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);

        let err = pg_lsn_mii(mcx, 1, &nbytes(mcx, "2")).unwrap_err();
        assert_eq!(err.message(), "pg_lsn out of range");
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn pli_and_mii_reject_nan_and_infinity() {
        let ctx = MemoryContext::new("lsn-nan");
        let mcx = ctx.mcx();
        let base = pg_lsn_in_internal("0/16AE7F7").0;

        let err = pg_lsn_pli(mcx, base, &nbytes(mcx, "NaN")).unwrap_err();
        assert_eq!(err.message(), "cannot add NaN to pg_lsn");
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);

        let err = pg_lsn_mii(mcx, base, &nbytes(mcx, "NaN")).unwrap_err();
        assert_eq!(err.message(), "cannot subtract NaN from pg_lsn");
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);

        // Infinity flows past the NaN gate and is caught by numeric_pg_lsn.
        let err = pg_lsn_pli(mcx, base, &nbytes(mcx, "Infinity")).unwrap_err();
        assert_eq!(err.message(), "cannot convert infinity to pg_lsn");
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    }
}
