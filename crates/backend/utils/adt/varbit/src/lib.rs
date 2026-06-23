#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the workspace-wide `PgResult` (== `Result<_,
// PgError>`); boxing the error locally would diverge from sibling crates.
#![allow(clippy::result_large_err)]

//! Port of `src/backend/utils/adt/varbit.c`: the SQL datatypes `BIT(n)` and
//! `BIT VARYING(n)` (a.k.a. `bit`/`varbit`). Covers I/O
//! (`bit_in`/`bit_out`/`bit_recv`/`bit_send` and the `varbit_*` variants),
//! typmod in/out, the `bit()`/`varbit()` length-coercion casts, the comparison
//! operators (`biteq`/`bitne`/`bitlt`/`bitle`/`bitgt`/`bitge`/`bitcmp`),
//! the bitwise logical ops (`bit_and`/`bit_or`/`bitxor`/`bitnot`), shifts
//! (`bitshiftleft`/`bitshiftright`), concatenation/substring/overlay
//! (`bitcat`/`bitsubstr`/`bitsubstr_no_len`/`bitoverlay`/`bitoverlay_no_len`),
//! `bit_count`, `bitlength`/`bitoctetlength`, `bitposition`,
//! `bitsetbit`/`bitgetbit`, and the int4/int8 casts
//! (`bitfromint4`/`bittoint4`/`bitfromint8`/`bittoint8`).
//!
//! # Carrier model (repo convention)
//!
//! A `VarBit` in C is a varlena struct: a 4-byte varlena length word, an `int32
//! bit_len` (number of valid bits), then `bit_dat[]`, the most-significant-byte-
//! first bit string. The byte count of the data section is exactly
//! `ceil(bit_len / 8)`; if `bit_len` is not a multiple of 8 the low-order pad
//! bits of the last byte MUST be zero (`bit_cmp` relies on this).
//!
//! Following the repo convention (header lives only at the Datum/FFI boundary,
//! like `backend-utils-adt-varlena`'s `cstring_to_text_with_len`), a `varbit`
//! *value* is carried as [`VarBit`] = `{ bit_len, data }`, where `data` is the
//! header-less `VARBITS` payload (`PgVec<'mcx, u8>` charged to the caller's
//! [`::mcx::Mcx`]). Functions that only *read* a value take [`VarBitRef`]
//! (`{ bit_len, data: &[u8] }`), the detoasted payload the fmgr glue produces.
//!
//! `*send` returns a full [`::datum::Bytea`] image (header + payload), built
//! through `pq_begintypsend`/`pq_endtypsend`.
//!
//! # The fmgr / `Datum` boundary
//!
//! C's SQL wrappers take `PG_FUNCTION_ARGS` and return `Datum`; per the
//! project-wide deferral each function here takes the already-unwrapped
//! arguments and returns an owned/typed value. No `*mut`/`extern "C"`/`Datum`
//! appears. The bare-word PGFunction registry and `varbit_support` (planner
//! support node, `supportnodes.h`) are deferred project-wide.

extern crate alloc;

use alloc::format;
use alloc::string::String;

use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::datum::Bytea;
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_ARRAY_SUBSCRIPT_ERROR,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_STRING_DATA_LENGTH_MISMATCH,
    ERRCODE_STRING_DATA_RIGHT_TRUNCATION, ERRCODE_SUBSTRING_ERROR,
};
use ::stringinfo::StringInfo;

use pqformat as pq;
use mbutils_seams as mb;

pub mod fmgr_builtins;

/// Register this unit's fmgr builtins (C: `fmgr_builtins[]` rows) so by-OID
/// dispatch / `fmgr_isbuiltin` resolves them. Called by `seams-init::init_all`.
pub fn init_seams() {
    fmgr_builtins::register_varbit_builtins();

    // make_const's T_BitString arm: DirectFunctionCall3(bit_in, str, InvalidOid,
    // -1). Slot declared in backend-parser-small1-seams (parse_node.c is the
    // consumer), owned and installed here.
    small1_seams::bit_in::set(bit_in_to_varlena);
}

// ===========================================================================
// constants (varbit.h, c.h)
// ===========================================================================

/// `BITS_PER_BYTE` (c.h).
const BITS_PER_BYTE: i32 = 8;

/// `HIGHBIT` (c.h): `0x80`.
const HIGHBIT: u8 = 0x80;

/// `BITMASK` (varbit.h): mask covering exactly one byte.
const BITMASK: u8 = 0xFF;

/// `MaxAttrSize` (htup_details.h): `10 * 1024 * 1024`.
const MAX_ATTR_SIZE: i32 = 10 * 1024 * 1024;

/// `VARBITMAXLEN` (varbit.h): `INT_MAX - BITS_PER_BYTE + 1`.
const VARBITMAXLEN: i32 = i32::MAX - BITS_PER_BYTE + 1;

/// `IS_HIGHBIT_SET(c)` (c.h).
#[inline]
fn is_highbit_set(c: u8) -> bool {
    (c & HIGHBIT) != 0
}

// ===========================================================================
// carrier types
// ===========================================================================

/// An owned `varbit`/`bit` value: `bit_len` valid bits and the header-less
/// `VARBITS` payload (exactly `ceil(bit_len/8)` bytes, last byte zero-padded).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VarBit<'mcx> {
    pub bit_len: i32,
    pub data: PgVec<'mcx, u8>,
}

/// A borrowed `varbit`/`bit` value (the detoasted argument form).
#[derive(Clone, Copy, Debug)]
pub struct VarBitRef<'a> {
    pub bit_len: i32,
    pub data: &'a [u8],
}

impl<'mcx> VarBit<'mcx> {
    fn as_ref(&self) -> VarBitRef<'_> {
        VarBitRef {
            bit_len: self.bit_len,
            data: &self.data,
        }
    }
}

impl<'a> VarBitRef<'a> {
    pub fn new(bit_len: i32, data: &'a [u8]) -> Self {
        VarBitRef { bit_len, data }
    }

    /// `VARBITLEN` — number of valid bits.
    #[inline]
    fn varbitlen(&self) -> i32 {
        self.bit_len
    }

    /// `VARBITBYTES` — number of bytes in the data section.
    #[inline]
    fn varbitbytes(&self) -> i32 {
        self.data.len() as i32
    }

    /// `VARBITPAD` — number of pad bits in the last byte.
    #[inline]
    fn varbitpad(&self) -> i32 {
        self.varbitbytes() * BITS_PER_BYTE - self.bit_len
    }
}

/// `VARBITTOTALLEN(BITLEN)` — number of bytes needed for the whole varlena
/// struct (header + data). Used in C to size the `palloc`; here we only need
/// the *data* section size, but keep the helper to mirror the bound checks.
#[inline]
fn varbit_data_bytes(bitlen: i32) -> i32 {
    (bitlen + BITS_PER_BYTE - 1) / BITS_PER_BYTE
}

/// Allocate a zero-filled data vector of `nbytes` bytes (C: `palloc0` of the
/// data section).
fn palloc0_data<'mcx>(mcx: Mcx<'mcx>, nbytes: i32) -> PgResult<PgVec<'mcx, u8>> {
    let n = nbytes.max(0) as usize;
    let mut v = vec_with_capacity_in(mcx, n)?;
    v.resize(n, 0u8);
    Ok(v)
}

/// `VARBIT_PAD` — mask off any bits that should be zero in the last byte.
fn varbit_pad(data: &mut [u8], bit_len: i32) {
    let bytelen = data.len() as i32;
    let pad = bytelen * BITS_PER_BYTE - bit_len;
    debug_assert!((0..BITS_PER_BYTE).contains(&pad));
    if pad > 0 {
        let last = data.len() - 1;
        data[last] &= BITMASK << pad;
    }
}

/// `cstring`-style result: copy bytes plus a NUL terminator into `mcx` (mirror
/// of the repo `cstring_to_text`-less helper used by typmodout / out funcs).
fn cstring<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut v = vec_with_capacity_in(mcx, bytes.len() + 1)?;
    v.extend_from_slice(bytes);
    v.push(0u8);
    Ok(v)
}

// ===========================================================================
// typmod helpers (shared by bit/varbit)
// ===========================================================================

/// `anybit_typmodin` (varbit.c:89) — common code for `bittypmodin` and
/// `varbittypmodin`. The `ArrayGetIntegerTypmods` decode is done by the caller;
/// `tl` is the decoded modifier list.
fn anybit_typmodin(tl: &[i32], typename: &str) -> PgResult<i32> {
    // we're not too tense about good error message here because grammar
    // shouldn't allow wrong number of modifiers for BIT
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
    if tl0 > MAX_ATTR_SIZE * BITS_PER_BYTE {
        return Err(PgError::error(format!(
            "length for type {} cannot exceed {}",
            typename,
            MAX_ATTR_SIZE * BITS_PER_BYTE
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    Ok(tl0)
}

/// `anybit_typmodout` (varbit.c:126) — `"(%d)"` or empty cstring.
fn anybit_typmodout<'mcx>(mcx: Mcx<'mcx>, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    if typmod >= 0 {
        cstring(mcx, format!("({typmod})").as_bytes())
    } else {
        cstring(mcx, b"")
    }
}

// ===========================================================================
// bit_in / varbit_in (shared parsing)
// ===========================================================================

/// Parse a bit/hex string into the data section. `sp` is the body of the input
/// (after the optional `b`/`x` prefix), `bit_not_hex` selects the format, and
/// `data` is the pre-zeroed destination of the correct length. Soft errors
/// route through `escontext`; on a soft error `Ok(false)` is returned.
fn parse_bitstring(
    sp: &[u8],
    bit_not_hex: bool,
    data: &mut [u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    let mut r = 0usize; // index into data
    if bit_not_hex {
        // Parse the bit representation of the string.
        let mut x = HIGHBIT;
        for (idx, &ch) in sp.iter().enumerate() {
            if ch == b'1' {
                data[r] |= x;
            } else if ch != b'0' {
                let mblen = mb::pg_mblen_range::call(&sp[idx..])?.max(1) as usize;
                let frag = String::from_utf8_lossy(&sp[idx..idx + mblen.min(sp.len() - idx)]);
                ereturn(
                    escontext,
                    false,
                    PgError::error(format!("\"{frag}\" is not a valid binary digit"))
                        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
                )?;
                return Ok(false);
            }
            x >>= 1;
            if x == 0 {
                x = HIGHBIT;
                r += 1;
            }
        }
    } else {
        // Parse the hex representation of the string.
        let mut bc = false;
        for (idx, &ch) in sp.iter().enumerate() {
            let x: u8 = if ch.is_ascii_digit() {
                ch - b'0'
            } else if (b'A'..=b'F').contains(&ch) {
                ch - b'A' + 10
            } else if (b'a'..=b'f').contains(&ch) {
                ch - b'a' + 10
            } else {
                let mblen = mb::pg_mblen_range::call(&sp[idx..])?.max(1) as usize;
                let frag = String::from_utf8_lossy(&sp[idx..idx + mblen.min(sp.len() - idx)]);
                ereturn(
                    escontext,
                    false,
                    PgError::error(format!("\"{frag}\" is not a valid hexadecimal digit"))
                        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
                )?;
                return Ok(false);
            };

            if bc {
                data[r] |= x;
                r += 1;
                bc = false;
            } else {
                data[r] = x << 4;
                bc = true;
            }
        }
    }
    Ok(true)
}

/// Split the input cstring into `(bit_not_hex, body)` per the leading `b`/`x`
/// marker (varbit.c:166).
fn classify_input(input: &[u8]) -> (bool, &[u8]) {
    match input.first() {
        Some(b'b') | Some(b'B') => (true, &input[1..]),
        Some(b'x') | Some(b'X') => (false, &input[1..]),
        // Otherwise it's binary. cast('1001' as bit) works transparently.
        _ => (true, input),
    }
}

/// `bit_in` (varbit.c:146) — cstring -> `bit`. `atttypmod` is the *exact*
/// length to force the bitstring to. Soft errors return `Ok(None)`.
pub fn bit_in<'mcx>(
    mcx: Mcx<'mcx>,
    input_string: &[u8],
    atttypmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<VarBit<'mcx>>> {
    let (bit_not_hex, sp) = classify_input(input_string);

    // Determine bitlength from input string.
    let slen = sp.len() as i32;
    let bitlen;
    if bit_not_hex {
        bitlen = slen;
    } else {
        if slen > VARBITMAXLEN / 4 {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                PgError::error(format!(
                    "bit string length exceeds the maximum allowed ({VARBITMAXLEN})"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }
        bitlen = slen * 4;
    }

    // If atttypmod is supplied, the bitstring must fit exactly.
    let atttypmod = if atttypmod <= 0 {
        bitlen
    } else if bitlen != atttypmod {
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error(format!(
                "bit string length {bitlen} does not match type bit({atttypmod})"
            ))
            .with_sqlstate(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
        );
    } else {
        atttypmod
    };

    let mut data = palloc0_data(mcx, varbit_data_bytes(atttypmod))?;
    if !parse_bitstring(sp, bit_not_hex, &mut data, escontext.as_deref_mut())? {
        return Ok(None);
    }

    Ok(Some(VarBit {
        bit_len: atttypmod,
        data,
    }))
}

/// `DirectFunctionCall3(bit_in, str, InvalidOid, -1)` as a by-reference `Datum`:
/// parse a `bit` literal (hard error, typmod -1) and serialise the result into
/// the full on-disk `VarBit` varlena byte image `[varsize_le | bit_len_le | data]`
/// (mirror of `fmgr_builtins::encode_varbit`). Installed into
/// `backend-parser-small1-seams::bit_in` for `make_const`'s `T_BitString` arm.
pub fn bit_in_to_varlena<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let v = bit_in(mcx, s, -1, None)?
        .expect("bit_in with no soft-error context returns Some or errors");
    let total = 8 + v.data.len();
    let mut out: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, total)?;
    out.extend_from_slice(&(total as u32).to_le_bytes());
    out.extend_from_slice(&v.bit_len.to_le_bytes());
    out.extend_from_slice(&v.data);
    Ok(out)
}

/// `varbit_in` (varbit.c:451) — cstring -> `varbit`. `atttypmod` is the
/// *maximum* length. Soft errors return `Ok(None)`.
pub fn varbit_in<'mcx>(
    mcx: Mcx<'mcx>,
    input_string: &[u8],
    atttypmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<VarBit<'mcx>>> {
    let (bit_not_hex, sp) = classify_input(input_string);

    let slen = sp.len() as i32;
    let bitlen;
    if bit_not_hex {
        bitlen = slen;
    } else {
        if slen > VARBITMAXLEN / 4 {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                PgError::error(format!(
                    "bit string length exceeds the maximum allowed ({VARBITMAXLEN})"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }
        bitlen = slen * 4;
    }

    let atttypmod = if atttypmod <= 0 {
        bitlen
    } else if bitlen > atttypmod {
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error(format!(
                "bit string too long for type bit varying({atttypmod})"
            ))
            .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
        );
    } else {
        atttypmod
    };

    let result_bitlen = bitlen.min(atttypmod);
    // The data section is sized to bitlen (the full input); VARBITLEN is set to
    // Min(bitlen, atttypmod), but since the too-long case errored, these match.
    let mut data = palloc0_data(mcx, varbit_data_bytes(bitlen))?;
    if !parse_bitstring(sp, bit_not_hex, &mut data, escontext.as_deref_mut())? {
        return Ok(None);
    }

    Ok(Some(VarBit {
        bit_len: result_bitlen,
        data,
    }))
}

// ===========================================================================
// out
// ===========================================================================

/// `varbit_out` (varbit.c:586) — prints the bit string as `0`/`1` to preserve
/// length accurately. `bit_out` (varbit.c:280) shares this code. Result is the
/// NUL-terminated cstring bytes.
pub fn varbit_out<'mcx>(mcx: Mcx<'mcx>, s: VarBitRef<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let len = s.varbitlen();
    let mut result = vec_with_capacity_in(mcx, (len + 1).max(0) as usize)?;
    let sp = s.data;
    let mut spi = 0usize;

    let mut i = 0i32;
    while i <= len - BITS_PER_BYTE {
        // print full bytes
        let mut x = sp[spi];
        for _ in 0..BITS_PER_BYTE {
            result.push(if is_highbit_set(x) { b'1' } else { b'0' });
            x <<= 1;
        }
        spi += 1;
        i += BITS_PER_BYTE;
    }
    if i < len {
        // print the last partial byte
        let mut x = sp[spi];
        let mut k = i;
        while k < len {
            result.push(if is_highbit_set(x) { b'1' } else { b'0' });
            x <<= 1;
            k += 1;
        }
    }
    result.push(0u8);

    Ok(result)
}

/// `bit_out` (varbit.c:280) — same as `varbit_out`.
pub fn bit_out<'mcx>(mcx: Mcx<'mcx>, s: VarBitRef<'_>) -> PgResult<PgVec<'mcx, u8>> {
    varbit_out(mcx, s)
}

// ===========================================================================
// recv / send
// ===========================================================================

/// `bit_recv` (varbit.c:330) — external binary format -> `bit`. `atttypmod` is
/// the exact length.
pub fn bit_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    atttypmod: i32,
) -> PgResult<VarBit<'mcx>> {
    let bitlen = pq::pq_getmsgint(buf, 4)? as i32;
    if bitlen < 0 || bitlen > VARBITMAXLEN {
        return Err(PgError::error("invalid length in external bit string")
            .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
    }

    if atttypmod > 0 && bitlen != atttypmod {
        return Err(PgError::error(format!(
            "bit string length {bitlen} does not match type bit({atttypmod})"
        ))
        .with_sqlstate(ERRCODE_STRING_DATA_LENGTH_MISMATCH));
    }

    let nbytes = varbit_data_bytes(bitlen);
    let mut data = vec_with_capacity_in(mcx, nbytes.max(0) as usize)?;
    data.resize(nbytes.max(0) as usize, 0u8);
    pq::pq_copymsgbytes(buf, &mut data)?;

    // Make sure last byte is correctly zero-padded.
    if bitlen > 0 {
        varbit_pad(&mut data, bitlen);
    }

    Ok(VarBit {
        bit_len: bitlen,
        data,
    })
}

/// `varbit_recv` (varbit.c:635) — external binary format -> `varbit`.
/// `atttypmod` is the maximum length.
pub fn varbit_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    atttypmod: i32,
) -> PgResult<VarBit<'mcx>> {
    let bitlen = pq::pq_getmsgint(buf, 4)? as i32;
    if bitlen < 0 || bitlen > VARBITMAXLEN {
        return Err(PgError::error("invalid length in external bit string")
            .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
    }

    if atttypmod > 0 && bitlen > atttypmod {
        return Err(PgError::error(format!(
            "bit string too long for type bit varying({atttypmod})"
        ))
        .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION));
    }

    let nbytes = varbit_data_bytes(bitlen);
    let mut data = vec_with_capacity_in(mcx, nbytes.max(0) as usize)?;
    data.resize(nbytes.max(0) as usize, 0u8);
    pq::pq_copymsgbytes(buf, &mut data)?;

    if bitlen > 0 {
        varbit_pad(&mut data, bitlen);
    }

    Ok(VarBit {
        bit_len: bitlen,
        data,
    })
}

/// `varbit_send` (varbit.c:680) — `varbit` -> external binary format.
/// `bit_send` (varbit.c:375) shares this code. External format is the bitlen as
/// an int32, then the byte array.
pub fn varbit_send<'mcx>(mcx: Mcx<'mcx>, s: VarBitRef<'_>) -> PgResult<Bytea<'mcx>> {
    let mut buf = pq::pq_begintypsend(mcx)?;
    pq::pq_sendint32(&mut buf, s.varbitlen() as u32)?;
    pq::pq_sendbytes(&mut buf, s.data)?;
    Ok(pq::pq_endtypsend(buf))
}

/// `bit_send` (varbit.c:375) — same as `varbit_send`.
pub fn bit_send<'mcx>(mcx: Mcx<'mcx>, s: VarBitRef<'_>) -> PgResult<Bytea<'mcx>> {
    varbit_send(mcx, s)
}

// ===========================================================================
// length-coercion casts
// ===========================================================================

/// `bit()` (varbit.c:390) — coerce a `bit()` value to a specific length `len`.
/// On implicit cast a length mismatch errors; on explicit cast it truncates or
/// zero-pads. Returns a fresh value (or a clone of `arg` if no work needed).
pub fn bit<'mcx>(
    mcx: Mcx<'mcx>,
    arg: VarBitRef<'_>,
    len: i32,
    is_explicit: bool,
) -> PgResult<VarBit<'mcx>> {
    // No work if typmod is invalid or supplied data matches it already.
    if len <= 0 || len > VARBITMAXLEN || len == arg.varbitlen() {
        return Ok(VarBit {
            bit_len: arg.bit_len,
            data: ::mcx::slice_in(mcx, arg.data)?,
        });
    }

    if !is_explicit {
        return Err(PgError::error(format!(
            "bit string length {} does not match type bit({})",
            arg.varbitlen(),
            len
        ))
        .with_sqlstate(ERRCODE_STRING_DATA_LENGTH_MISMATCH));
    }

    let rbytes = varbit_data_bytes(len);
    // palloc0 so the string is zero-padded.
    let mut data = palloc0_data(mcx, rbytes)?;
    let copy = (data.len()).min(arg.data.len());
    data[..copy].copy_from_slice(&arg.data[..copy]);

    // Make sure last byte is zero-padded if needed.
    if len > 0 {
        varbit_pad(&mut data, len);
    }

    Ok(VarBit { bit_len: len, data })
}

/// `varbit()` (varbit.c:741) — coerce a `varbit()` value to a maximum length
/// `len`. Implicit cast errors if too long; explicit cast truncates.
pub fn varbit<'mcx>(
    mcx: Mcx<'mcx>,
    arg: VarBitRef<'_>,
    len: i32,
    is_explicit: bool,
) -> PgResult<VarBit<'mcx>> {
    // No work if typmod is invalid or supplied data matches it already.
    if len <= 0 || len >= arg.varbitlen() {
        return Ok(VarBit {
            bit_len: arg.bit_len,
            data: ::mcx::slice_in(mcx, arg.data)?,
        });
    }

    if !is_explicit {
        return Err(PgError::error(format!(
            "bit string too long for type bit varying({len})"
        ))
        .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION));
    }

    let rbytes = varbit_data_bytes(len);
    let mut data = vec_with_capacity_in(mcx, rbytes.max(0) as usize)?;
    data.extend_from_slice(&arg.data[..rbytes.max(0) as usize]);

    // Make sure last byte is correctly zero-padded.
    if len > 0 {
        varbit_pad(&mut data, len);
    }

    Ok(VarBit { bit_len: len, data })
}

// ===========================================================================
// typmod in/out SQL entry points
// ===========================================================================

/// `bittypmodin` (varbit.c:428). `ta` is the raw `cstring[]` array image.
pub fn bittypmodin(mcx: Mcx<'_>, ta: &[u8]) -> PgResult<i32> {
    let tl = arrayutils::array_get_integer_typmods(mcx, ta)?;
    anybit_typmodin(&tl, "bit")
}

/// `bittypmodout` (varbit.c:436).
pub fn bittypmodout<'mcx>(mcx: Mcx<'mcx>, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    anybit_typmodout(mcx, typmod)
}

/// `varbittypmodin` (varbit.c:773).
pub fn varbittypmodin(mcx: Mcx<'_>, ta: &[u8]) -> PgResult<i32> {
    let tl = arrayutils::array_get_integer_typmods(mcx, ta)?;
    anybit_typmodin(&tl, "varbit")
}

/// `varbittypmodout` (varbit.c:781).
pub fn varbittypmodout<'mcx>(mcx: Mcx<'mcx>, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    anybit_typmodout(mcx, typmod)
}

// ===========================================================================
// comparison
// ===========================================================================

/// `bit_cmp` (varbit.c:817) — compares two bitstrings, returns <0/0/>0.
fn bit_cmp(arg1: VarBitRef<'_>, arg2: VarBitRef<'_>) -> i32 {
    let bytelen1 = arg1.varbitbytes() as usize;
    let bytelen2 = arg2.varbitbytes() as usize;

    let n = bytelen1.min(bytelen2);
    let mut cmp = match arg1.data[..n].cmp(&arg2.data[..n]) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    };
    if cmp == 0 {
        let bitlen1 = arg1.varbitlen();
        let bitlen2 = arg2.varbitlen();
        if bitlen1 != bitlen2 {
            cmp = if bitlen1 < bitlen2 { -1 } else { 1 };
        }
    }
    cmp
}

/// `biteq` (varbit.c:840).
pub fn biteq(arg1: VarBitRef<'_>, arg2: VarBitRef<'_>) -> bool {
    // fast path for different-length inputs
    if arg1.varbitlen() != arg2.varbitlen() {
        false
    } else {
        bit_cmp(arg1, arg2) == 0
    }
}

/// `bitne` (varbit.c:864).
pub fn bitne(arg1: VarBitRef<'_>, arg2: VarBitRef<'_>) -> bool {
    if arg1.varbitlen() != arg2.varbitlen() {
        true
    } else {
        bit_cmp(arg1, arg2) != 0
    }
}

/// `bitlt` (varbit.c:888).
pub fn bitlt(arg1: VarBitRef<'_>, arg2: VarBitRef<'_>) -> bool {
    bit_cmp(arg1, arg2) < 0
}

/// `bitle` (varbit.c:903).
pub fn bitle(arg1: VarBitRef<'_>, arg2: VarBitRef<'_>) -> bool {
    bit_cmp(arg1, arg2) <= 0
}

/// `bitgt` (varbit.c:918).
pub fn bitgt(arg1: VarBitRef<'_>, arg2: VarBitRef<'_>) -> bool {
    bit_cmp(arg1, arg2) > 0
}

/// `bitge` (varbit.c:933).
pub fn bitge(arg1: VarBitRef<'_>, arg2: VarBitRef<'_>) -> bool {
    bit_cmp(arg1, arg2) >= 0
}

/// `bitcmp` (varbit.c:948).
pub fn bitcmp(arg1: VarBitRef<'_>, arg2: VarBitRef<'_>) -> i32 {
    bit_cmp(arg1, arg2)
}

// ===========================================================================
// concatenation
// ===========================================================================

/// `bit_catenate` (varbit.c:976) — concatenation guts.
fn bit_catenate<'mcx>(
    mcx: Mcx<'mcx>,
    arg1: VarBitRef<'_>,
    arg2: VarBitRef<'_>,
) -> PgResult<VarBit<'mcx>> {
    let bitlen1 = arg1.varbitlen();
    let bitlen2 = arg2.varbitlen();

    if bitlen1 > VARBITMAXLEN - bitlen2 {
        return Err(PgError::error(format!(
            "bit string length exceeds the maximum allowed ({VARBITMAXLEN})"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }
    let total_bits = bitlen1 + bitlen2;
    let nbytes = varbit_data_bytes(total_bits);

    let mut data = palloc0_data(mcx, nbytes)?;

    let bytelen1 = arg1.varbitbytes() as usize;
    let bytelen2 = arg2.varbitbytes() as usize;

    // Copy the first bitstring in.
    data[..bytelen1].copy_from_slice(arg1.data);

    // Copy the second bit string.
    let bit1pad = arg1.varbitpad();
    if bit1pad == 0 {
        data[bytelen1..bytelen1 + bytelen2].copy_from_slice(arg2.data);
    } else if bitlen2 > 0 {
        // We need to shift all the bits to fit.
        let bit2shift = (BITS_PER_BYTE - bit1pad) as u32;
        // pr starts at VARBITS(result) + VARBITBYTES(arg1) - 1
        let mut pr = bytelen1 - 1;
        let dlen = data.len();
        for &pa in arg2.data.iter() {
            data[pr] |= (pa >> bit2shift) & BITMASK;
            pr += 1;
            if pr < dlen {
                data[pr] = (pa << (bit1pad as u32)) & BITMASK;
            }
        }
    }

    // The pad bits should be already zero at this point.

    Ok(VarBit {
        bit_len: total_bits,
        data,
    })
}

/// `bitcat` (varbit.c:967).
pub fn bitcat<'mcx>(
    mcx: Mcx<'mcx>,
    arg1: VarBitRef<'_>,
    arg2: VarBitRef<'_>,
) -> PgResult<VarBit<'mcx>> {
    bit_catenate(mcx, arg1, arg2)
}

// ===========================================================================
// substring
// ===========================================================================

/// `bitsubstring` (varbit.c:1054) — substring guts. `s` is 1-based.
fn bitsubstring<'mcx>(
    mcx: Mcx<'mcx>,
    arg: VarBitRef<'_>,
    s: i32,
    l: i32,
    length_not_specified: bool,
) -> PgResult<VarBit<'mcx>> {
    let bitlen = arg.varbitlen();
    let s1 = s.max(1);

    let e1: i32;
    if length_not_specified {
        e1 = bitlen + 1;
    } else if l < 0 {
        // SQL99 says to throw an error for E < S, i.e., negative length.
        return Err(PgError::error("negative substring length not allowed")
            .with_sqlstate(ERRCODE_SUBSTRING_ERROR));
    } else {
        match s.checked_add(l) {
            None => {
                // S + L overflowed; the substring runs to end of string.
                e1 = bitlen + 1;
            }
            Some(e) => {
                e1 = e.min(bitlen + 1);
            }
        }
    }

    if s1 > bitlen || e1 <= s1 {
        // Need to return a zero-length bitstring.
        let data = palloc0_data(mcx, varbit_data_bytes(0))?;
        return Ok(VarBit { bit_len: 0, data });
    }

    // True substring from position s1-1 to e1-1.
    let rbitlen = e1 - s1;
    let len = varbit_data_bytes(rbitlen) as usize;
    let mut data = palloc0_data(mcx, len as i32)?;
    let bit_len = rbitlen;

    // Are we copying from a byte boundary?
    if (s1 - 1) % BITS_PER_BYTE == 0 {
        // Yep, we are copying bytes.
        let off = ((s1 - 1) / BITS_PER_BYTE) as usize;
        data[..len].copy_from_slice(&arg.data[off..off + len]);
    } else {
        // Figure out how much we need to shift the sequence by.
        let ishift = ((s1 - 1) % BITS_PER_BYTE) as u32;
        let mut psi = ((s1 - 1) / BITS_PER_BYTE) as usize;
        let arglen = arg.data.len();
        for ri in 0..len {
            let mut byte = (arg.data[psi] << ishift) & BITMASK;
            psi += 1;
            if psi < arglen {
                byte |= arg.data[psi] >> (BITS_PER_BYTE as u32 - ishift);
            }
            data[ri] = byte;
        }
    }

    // Make sure last byte is correctly zero-padded.
    if bit_len > 0 {
        varbit_pad(&mut data, bit_len);
    }

    Ok(VarBit { bit_len, data })
}

/// `bitsubstr` (varbit.c:1037).
pub fn bitsubstr<'mcx>(
    mcx: Mcx<'mcx>,
    arg: VarBitRef<'_>,
    s: i32,
    l: i32,
) -> PgResult<VarBit<'mcx>> {
    bitsubstring(mcx, arg, s, l, false)
}

/// `bitsubstr_no_len` (varbit.c:1046).
pub fn bitsubstr_no_len<'mcx>(
    mcx: Mcx<'mcx>,
    arg: VarBitRef<'_>,
    s: i32,
) -> PgResult<VarBit<'mcx>> {
    bitsubstring(mcx, arg, s, -1, true)
}

// ===========================================================================
// overlay
// ===========================================================================

/// `bit_overlay` (varbit.c:1175) — replace substring of `t1` with `t2`.
fn bit_overlay<'mcx>(
    mcx: Mcx<'mcx>,
    t1: VarBitRef<'_>,
    t2: VarBitRef<'_>,
    sp: i32,
    sl: i32,
) -> PgResult<VarBit<'mcx>> {
    if sp <= 0 {
        return Err(PgError::error("negative substring length not allowed")
            .with_sqlstate(ERRCODE_SUBSTRING_ERROR));
    }
    let sp_pl_sl = match sp.checked_add(sl) {
        Some(v) => v,
        None => {
            return Err(PgError::error("integer out of range")
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
        }
    };

    let s1 = bitsubstring(mcx, t1, 1, sp - 1, false)?;
    let s2 = bitsubstring(mcx, t1, sp_pl_sl, -1, true)?;
    let result = bit_catenate(mcx, s1.as_ref(), t2)?;
    let result = bit_catenate(mcx, result.as_ref(), s2.as_ref())?;

    Ok(result)
}

/// `bitoverlay` (varbit.c:1152).
pub fn bitoverlay<'mcx>(
    mcx: Mcx<'mcx>,
    t1: VarBitRef<'_>,
    t2: VarBitRef<'_>,
    sp: i32,
    sl: i32,
) -> PgResult<VarBit<'mcx>> {
    bit_overlay(mcx, t1, t2, sp, sl)
}

/// `bitoverlay_no_len` (varbit.c:1163) — `sl` defaults to `length(t2)`.
pub fn bitoverlay_no_len<'mcx>(
    mcx: Mcx<'mcx>,
    t1: VarBitRef<'_>,
    t2: VarBitRef<'_>,
    sp: i32,
) -> PgResult<VarBit<'mcx>> {
    let sl = t2.varbitlen();
    bit_overlay(mcx, t1, t2, sp, sl)
}

// ===========================================================================
// bit_count, length
// ===========================================================================

/// `bit_bit_count` (varbit.c:1210) — number of set bits (C: `pg_popcount`).
pub fn bit_bit_count(arg: VarBitRef<'_>) -> i64 {
    arg.data.iter().map(|b| b.count_ones() as i64).sum()
}

/// `bitlength` (varbit.c:1222).
pub fn bitlength(arg: VarBitRef<'_>) -> i32 {
    arg.varbitlen()
}

/// `bitoctetlength` (varbit.c:1230).
pub fn bitoctetlength(arg: VarBitRef<'_>) -> i32 {
    arg.varbitbytes()
}

// ===========================================================================
// bitwise logical ops
// ===========================================================================

/// `bit_and` (varbit.c:1242).
pub fn bit_and<'mcx>(
    mcx: Mcx<'mcx>,
    arg1: VarBitRef<'_>,
    arg2: VarBitRef<'_>,
) -> PgResult<VarBit<'mcx>> {
    if arg1.varbitlen() != arg2.varbitlen() {
        return Err(PgError::error("cannot AND bit strings of different sizes")
            .with_sqlstate(ERRCODE_STRING_DATA_LENGTH_MISMATCH));
    }
    let mut data = vec_with_capacity_in(mcx, arg1.data.len())?;
    for i in 0..arg1.data.len() {
        data.push(arg1.data[i] & arg2.data[i]);
    }
    // Padding is not needed as & of 0 pads is 0.
    Ok(VarBit {
        bit_len: arg1.varbitlen(),
        data,
    })
}

/// `bit_or` (varbit.c:1283).
pub fn bit_or<'mcx>(
    mcx: Mcx<'mcx>,
    arg1: VarBitRef<'_>,
    arg2: VarBitRef<'_>,
) -> PgResult<VarBit<'mcx>> {
    if arg1.varbitlen() != arg2.varbitlen() {
        return Err(PgError::error("cannot OR bit strings of different sizes")
            .with_sqlstate(ERRCODE_STRING_DATA_LENGTH_MISMATCH));
    }
    let mut data = vec_with_capacity_in(mcx, arg1.data.len())?;
    for i in 0..arg1.data.len() {
        data.push(arg1.data[i] | arg2.data[i]);
    }
    // Padding is not needed as | of 0 pads is 0.
    Ok(VarBit {
        bit_len: arg1.varbitlen(),
        data,
    })
}

/// `bitxor` (varbit.c:1323).
pub fn bitxor<'mcx>(
    mcx: Mcx<'mcx>,
    arg1: VarBitRef<'_>,
    arg2: VarBitRef<'_>,
) -> PgResult<VarBit<'mcx>> {
    if arg1.varbitlen() != arg2.varbitlen() {
        return Err(PgError::error("cannot XOR bit strings of different sizes")
            .with_sqlstate(ERRCODE_STRING_DATA_LENGTH_MISMATCH));
    }
    let mut data = vec_with_capacity_in(mcx, arg1.data.len())?;
    for i in 0..arg1.data.len() {
        data.push(arg1.data[i] ^ arg2.data[i]);
    }
    // Padding is not needed as ^ of 0 pads is 0.
    Ok(VarBit {
        bit_len: arg1.varbitlen(),
        data,
    })
}

/// `bitnot` (varbit.c:1364).
pub fn bitnot<'mcx>(mcx: Mcx<'mcx>, arg: VarBitRef<'_>) -> PgResult<VarBit<'mcx>> {
    let mut data = vec_with_capacity_in(mcx, arg.data.len())?;
    for &p in arg.data.iter() {
        data.push(!p);
    }
    // Must zero-pad the result, because extra bits are surely 1's here.
    if arg.varbitlen() > 0 {
        varbit_pad(&mut data, arg.varbitlen());
    }
    Ok(VarBit {
        bit_len: arg.varbitlen(),
        data,
    })
}

// ===========================================================================
// shifts
// ===========================================================================

/// `bitshiftleft` (varbit.c:1391) — shift towards the beginning of the string.
pub fn bitshiftleft<'mcx>(mcx: Mcx<'mcx>, arg: VarBitRef<'_>, shft: i32) -> PgResult<VarBit<'mcx>> {
    // Negative shift is a shift to the right.
    if shft < 0 {
        let mut shft = shft;
        if shft < -VARBITMAXLEN {
            shft = -VARBITMAXLEN;
        }
        return bitshiftright(mcx, arg, -shft);
    }

    let nbytes = arg.data.len();
    let mut data = vec_with_capacity_in(mcx, nbytes)?;
    data.resize(nbytes, 0u8);
    let bit_len = arg.varbitlen();

    // If we shifted all the bits out, return an all-zero string.
    if shft >= bit_len {
        // data already zeroed
        return Ok(VarBit { bit_len, data });
    }

    let byte_shift = (shft / BITS_PER_BYTE) as usize;
    let ishift = (shft % BITS_PER_BYTE) as u32;
    // p = VARBITS(arg) + byte_shift
    let dlen = data.len();

    if ishift == 0 {
        // Special case: we can do a memcpy.
        let len = nbytes - byte_shift;
        data[..len].copy_from_slice(&arg.data[byte_shift..byte_shift + len]);
        // MemSet(r + len, 0, byte_shift) — already zero.
    } else {
        // for (; p < VARBITEND(arg); r++)
        let mut pi = byte_shift;
        let mut ri = 0usize;
        while pi < nbytes {
            let mut byte = arg.data[pi] << ishift;
            pi += 1;
            if pi < nbytes {
                byte |= arg.data[pi] >> (BITS_PER_BYTE as u32 - ishift);
            }
            data[ri] = byte;
            ri += 1;
        }
        while ri < dlen {
            data[ri] = 0;
            ri += 1;
        }
    }

    // The pad bits should be already zero at this point.

    Ok(VarBit { bit_len, data })
}

/// `bitshiftright` (varbit.c:1458) — shift towards the end of the string.
pub fn bitshiftright<'mcx>(mcx: Mcx<'mcx>, arg: VarBitRef<'_>, shft: i32) -> PgResult<VarBit<'mcx>> {
    // Negative shift is a shift to the left.
    if shft < 0 {
        let mut shft = shft;
        if shft < -VARBITMAXLEN {
            shft = -VARBITMAXLEN;
        }
        return bitshiftleft(mcx, arg, -shft);
    }

    let nbytes = arg.data.len();
    let mut data = vec_with_capacity_in(mcx, nbytes)?;
    data.resize(nbytes, 0u8);
    let bit_len = arg.varbitlen();

    // If we shifted all the bits out, return an all-zero string.
    if shft >= bit_len {
        return Ok(VarBit { bit_len, data });
    }

    let byte_shift = (shft / BITS_PER_BYTE) as usize;
    let ishift = (shft % BITS_PER_BYTE) as u32;
    let dlen = data.len();

    // Set the first part of the result to 0 (already zeroed); r += byte_shift.
    let mut ri = byte_shift;
    let mut pi = 0usize;

    if ishift == 0 {
        // Special case: we can do a memcpy.
        let len = nbytes - byte_shift;
        data[ri..ri + len].copy_from_slice(&arg.data[..len]);
        let _ = ri; // C advances r past the copy; nothing further reads it
    } else {
        if ri < dlen {
            data[ri] = 0; // initialize first byte
        }
        while ri < dlen {
            data[ri] |= arg.data[pi] >> ishift;
            ri += 1;
            if ri < dlen {
                data[ri] = (arg.data[pi] << (BITS_PER_BYTE as u32 - ishift)) & BITMASK;
            }
            pi += 1;
        }
    }

    // We may have shifted 1's into the pad bits, so fix that. C uses
    // VARBIT_PAD_LAST(result, r) where r is one past the last written byte.
    if bit_len > 0 {
        varbit_pad(&mut data, bit_len);
    }

    Ok(VarBit { bit_len, data })
}

// ===========================================================================
// int <-> bit casts
// ===========================================================================

/// `bitfromint4` (varbit.c:1530).
pub fn bitfromint4<'mcx>(mcx: Mcx<'mcx>, a: i32, typmod: i32) -> PgResult<VarBit<'mcx>> {
    let typmod = if typmod <= 0 || typmod > VARBITMAXLEN {
        1 // default bit length
    } else {
        typmod
    };

    let nbytes = varbit_data_bytes(typmod);
    let mut data = vec_with_capacity_in(mcx, nbytes.max(0) as usize)?;
    data.resize(nbytes.max(0) as usize, 0u8);

    let mut ri = 0usize;
    let mut destbitsleft = typmod;
    let mut srcbitsleft = 32;
    // drop any input bits that don't fit
    srcbitsleft = srcbitsleft.min(destbitsleft);
    // sign-fill any excess bytes in output
    while destbitsleft >= srcbitsleft + 8 {
        data[ri] = if a < 0 { BITMASK } else { 0 };
        ri += 1;
        destbitsleft -= 8;
    }
    // store first fractional byte
    if destbitsleft > srcbitsleft {
        // C: unsigned int val = (unsigned int) (a >> (destbitsleft - 8));
        // arithmetic (sign-preserving) shift of the signed int, then cast.
        let mut val = (a >> (destbitsleft - 8)) as u32;
        // Force sign-fill in case the compiler implements >> as zero-fill.
        if a < 0 {
            val |= (u32::MAX) << (srcbitsleft + 8 - destbitsleft);
        }
        data[ri] = (val & BITMASK as u32) as u8;
        ri += 1;
        destbitsleft -= 8;
    }
    // store whole bytes
    while destbitsleft >= 8 {
        data[ri] = (((a >> (destbitsleft - 8)) as u32) & BITMASK as u32) as u8;
        ri += 1;
        destbitsleft -= 8;
    }
    // store last fractional byte
    if destbitsleft > 0 {
        data[ri] = (((a << (8 - destbitsleft)) as u32) & BITMASK as u32) as u8;
    }

    Ok(VarBit {
        bit_len: typmod,
        data,
    })
}

/// `bittoint4` (varbit.c:1585).
pub fn bittoint4(arg: VarBitRef<'_>) -> PgResult<i32> {
    // Check that the bit string is not too long (sizeof(uint32) * 8 = 32 bits).
    if arg.varbitlen() > (core::mem::size_of::<u32>() as i32) * BITS_PER_BYTE {
        return Err(PgError::error("integer out of range")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }

    let mut result: u32 = 0;
    for &r in arg.data.iter() {
        // C relies on uint32 wraparound when bit_len == 32 (4 bytes).
        result = result.wrapping_shl(BITS_PER_BYTE as u32);
        result |= r as u32;
    }
    // Shift the result to account for the padding at the end.
    result >>= arg.varbitpad() as u32;

    Ok(result as i32)
}

/// `bitfromint8` (varbit.c:1610).
pub fn bitfromint8<'mcx>(mcx: Mcx<'mcx>, a: i64, typmod: i32) -> PgResult<VarBit<'mcx>> {
    let typmod = if typmod <= 0 || typmod > VARBITMAXLEN {
        1 // default bit length
    } else {
        typmod
    };

    let nbytes = varbit_data_bytes(typmod);
    let mut data = vec_with_capacity_in(mcx, nbytes.max(0) as usize)?;
    data.resize(nbytes.max(0) as usize, 0u8);

    let mut ri = 0usize;
    let mut destbitsleft = typmod;
    let mut srcbitsleft = 64;
    srcbitsleft = srcbitsleft.min(destbitsleft);
    while destbitsleft >= srcbitsleft + 8 {
        data[ri] = if a < 0 { BITMASK } else { 0 };
        ri += 1;
        destbitsleft -= 8;
    }
    if destbitsleft > srcbitsleft {
        // C: unsigned int val = (unsigned int) (a >> (destbitsleft - 8));
        let mut val = (a >> (destbitsleft - 8)) as u32;
        if a < 0 {
            val |= (u32::MAX) << (srcbitsleft + 8 - destbitsleft);
        }
        data[ri] = (val & BITMASK as u32) as u8;
        ri += 1;
        destbitsleft -= 8;
    }
    while destbitsleft >= 8 {
        data[ri] = (((a >> (destbitsleft - 8)) as u64) & BITMASK as u64) as u8;
        ri += 1;
        destbitsleft -= 8;
    }
    if destbitsleft > 0 {
        data[ri] = (((a << (8 - destbitsleft)) as u64) & BITMASK as u64) as u8;
    }

    Ok(VarBit {
        bit_len: typmod,
        data,
    })
}

/// `bittoint8` (varbit.c:1665).
pub fn bittoint8(arg: VarBitRef<'_>) -> PgResult<i64> {
    if arg.varbitlen() > (core::mem::size_of::<u64>() as i32) * BITS_PER_BYTE {
        return Err(PgError::error("bigint out of range")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }

    let mut result: u64 = 0;
    for &r in arg.data.iter() {
        // C relies on uint64 wraparound when bit_len == 64 (8 bytes).
        result = result.wrapping_shl(BITS_PER_BYTE as u32);
        result |= r as u64;
    }
    result >>= arg.varbitpad() as u64;

    Ok(result as i64)
}

// ===========================================================================
// position
// ===========================================================================

/// `bitposition` (varbit.c:1697) — position of `substr` in `str` (1-based), or
/// 0 if not found; 1 if `substr` is empty.
pub fn bitposition(str: VarBitRef<'_>, substr: VarBitRef<'_>) -> i32 {
    let substr_length = substr.varbitlen();
    let str_length = str.varbitlen();

    // String has zero length or substring longer than string, return 0.
    if str_length == 0 || substr_length > str_length {
        return 0;
    }
    // zero-length substring means return 1.
    if substr_length == 0 {
        return 1;
    }

    let str_bytes = str.varbitbytes() as usize;
    let substr_bytes = substr.varbitbytes() as usize;

    // Initialise the padding masks.
    let end_mask: u8 = BITMASK << substr.varbitpad();
    let str_mask: u8 = BITMASK << str.varbitpad();

    let str_end = str_bytes; // VARBITEND(str) relative index
    let substr_end = substr_bytes;

    for i in 0..(str_bytes - substr_bytes + 1) {
        for is in 0..(BITS_PER_BYTE as usize) {
            let mut is_match = true;
            // p = VARBITS(str) + i
            let mut p = i;
            let mut mask1: u8 = BITMASK >> is;
            let mut mask2: u8 = !mask1;
            // for (s = VARBITS(substr); is_match && s < VARBITEND(substr); s++)
            let mut s = 0usize;
            while is_match && s < substr_end {
                let mut cmp = substr.data[s] >> is;
                if s == substr_end - 1 {
                    mask1 &= end_mask >> is;
                    if p == str_end - 1 {
                        // Check that there is enough of str left.
                        if mask1 & !str_mask != 0 {
                            is_match = false;
                            break;
                        }
                        mask1 &= str_mask;
                    }
                }
                is_match = ((cmp ^ str.data[p]) & mask1) == 0;
                if !is_match {
                    break;
                }
                // Move on to the next byte.
                p += 1;
                if p == str_end {
                    mask2 = if is == 0 {
                        // end_mask << (BITS_PER_BYTE - 0) == end_mask << 8 == 0
                        0
                    } else {
                        end_mask << (BITS_PER_BYTE as usize - is)
                    };
                    is_match = mask2 == 0;
                    break;
                }
                cmp = if is == 0 {
                    // *s << (BITS_PER_BYTE - 0) == *s << 8 == 0
                    0
                } else {
                    substr.data[s] << (BITS_PER_BYTE as usize - is)
                };
                if s == substr_end - 1 {
                    mask2 &= if is == 0 {
                        0
                    } else {
                        end_mask << (BITS_PER_BYTE as usize - is)
                    };
                    if p == str_end - 1 {
                        if mask2 & !str_mask != 0 {
                            is_match = false;
                            break;
                        }
                        mask2 &= str_mask;
                    }
                }
                is_match = ((cmp ^ str.data[p]) & mask2) == 0;
                s += 1;
            }
            // Have we found a match?
            if is_match {
                return (i as i32) * BITS_PER_BYTE + (is as i32) + 1;
            }
        }
    }
    0
}

// ===========================================================================
// get/set bit
// ===========================================================================

/// `bitsetbit` (varbit.c:1806) — return a copy with the Nth bit set to
/// `new_bit` (0-based, left-to-right).
pub fn bitsetbit<'mcx>(
    mcx: Mcx<'mcx>,
    arg1: VarBitRef<'_>,
    n: i32,
    new_bit: i32,
) -> PgResult<VarBit<'mcx>> {
    let bitlen = arg1.varbitlen();
    if n < 0 || n >= bitlen {
        return Err(PgError::error(format!(
            "bit index {} out of valid range (0..{})",
            n,
            bitlen - 1
        ))
        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    if new_bit != 0 && new_bit != 1 {
        return Err(
            PgError::error("new bit must be 0 or 1").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        );
    }

    let mut data = ::mcx::slice_in(mcx, arg1.data)?;

    let byte_no = (n / BITS_PER_BYTE) as usize;
    let bit_no = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE);

    if new_bit == 0 {
        data[byte_no] &= !(1u8 << bit_no);
    } else {
        data[byte_no] |= 1u8 << bit_no;
    }

    Ok(VarBit { bit_len: bitlen, data })
}

/// `bitgetbit` (varbit.c:1868) — value of the Nth bit (0 or 1; 0-based).
pub fn bitgetbit(arg1: VarBitRef<'_>, n: i32) -> PgResult<i32> {
    let bitlen = arg1.varbitlen();
    if n < 0 || n >= bitlen {
        return Err(PgError::error(format!(
            "bit index {} out of valid range (0..{})",
            n,
            bitlen - 1
        ))
        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    let byte_no = (n / BITS_PER_BYTE) as usize;
    let bit_no = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE);

    if arg1.data[byte_no] & (1u8 << bit_no) != 0 {
        Ok(1)
    } else {
        Ok(0)
    }
}

#[cfg(test)]
mod tests;
