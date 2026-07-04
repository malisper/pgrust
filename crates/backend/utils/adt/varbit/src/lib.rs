//! varbit.c, I/O slice: bit_in/bit_out/varbit_in/varbit_out. Comparison,
//! concatenation, substring, casts and the length-coercion functions stay
//! loud through the canonical fmgr table.
#![no_std]
extern crate alloc;

use alloc::format;

use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_STRING_DATA_LENGTH_MISMATCH, ERRCODE_STRING_DATA_RIGHT_TRUNCATION,
};
use ::types_fmgr::{
    cstring_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

const VARHDRSZ: usize = 4;
const VARBITHDRSZ: usize = 4;
const BITS_PER_BYTE: usize = 8;
const HIGHBIT: u8 = 0x80;
// varbit.h: INT_MAX - BITS_PER_BYTE + 1.
const VARBITMAXLEN: i64 = i32::MAX as i64 - 8 + 1;

const fn varbit_total_len(bitlen: usize) -> usize {
    bitlen.div_ceil(BITS_PER_BYTE) + VARHDRSZ + VARBITHDRSZ
}

// bit_in and varbit_in differ only in the typmod check; C keeps two copies.
fn bits_in<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    atttypmod: i32,
    fixed: bool,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let (bit_not_hex, sp) = match input.first() {
        Some(b'b') | Some(b'B') => (true, &input[1..]),
        Some(b'x') | Some(b'X') => (false, &input[1..]),
        _ => (true, input),
    };
    let slen = sp.len();
    let bitlen = if bit_not_hex {
        slen as i64
    } else {
        if slen as i64 > VARBITMAXLEN / 4 {
            return ereturn(escontext, None, too_long_err());
        }
        slen as i64 * 4
    };

    let atttypmod = if atttypmod <= 0 {
        bitlen
    } else if fixed && bitlen != atttypmod as i64 {
        return ereturn(escontext, None, length_mismatch_err(bitlen, atttypmod));
    } else if !fixed && bitlen > atttypmod as i64 {
        return ereturn(escontext, None, too_long_for_varying_err(atttypmod));
    } else {
        atttypmod as i64
    };

    let stored_bits = if fixed { atttypmod } else { bitlen.min(atttypmod) };
    let len = varbit_total_len(if fixed { atttypmod as usize } else { bitlen as usize });
    let mut out: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, len)?;
    out.extend_from_slice(&::datum::varlena::set_varsize_4b(len));
    out.extend_from_slice(&(stored_bits as i32).to_ne_bytes());
    for _ in (VARHDRSZ + VARBITHDRSZ)..len {
        out.push(0);
    }
    let r = &mut out[VARHDRSZ + VARBITHDRSZ..];
    if bit_not_hex {
        let mut x = HIGHBIT;
        let mut ri = 0usize;
        for &c in sp {
            if c == b'1' {
                r[ri] |= x;
            } else if c != b'0' {
                return ereturn(escontext, None, bad_digit_err(c, true));
            }
            x >>= 1;
            if x == 0 {
                x = HIGHBIT;
                ri += 1;
            }
        }
    } else {
        let mut bc = false;
        let mut ri = 0usize;
        for &c in sp {
            let x = match c {
                b'0'..=b'9' => c - b'0',
                b'A'..=b'F' => c - b'A' + 10,
                b'a'..=b'f' => c - b'a' + 10,
                _ => return ereturn(escontext, None, bad_digit_err(c, false)),
            };
            if bc {
                r[ri] |= x;
                ri += 1;
                bc = false;
            } else {
                r[ri] = x << 4;
                bc = true;
            }
        }
    }
    Ok(Some(out))
}

// `payload` is the varlena body: [bit_len i32][zero-padded bits].
pub fn bits_out<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let bitlen = i32::from_ne_bytes(payload[..VARBITHDRSZ].try_into().unwrap()) as usize;
    let sp = &payload[VARBITHDRSZ..];
    let mut out: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, bitlen + 1)?;
    for k in 0..bitlen {
        let byte = sp[k / BITS_PER_BYTE];
        let bit = byte << (k % BITS_PER_BYTE);
        out.push(if bit & HIGHBIT != 0 { b'1' } else { b'0' });
    }
    out.push(0);
    Ok(out)
}

#[cold]
#[inline(never)]
fn too_long_err() -> PgError {
    PgError::error(format!(
        "bit string length exceeds the maximum allowed ({VARBITMAXLEN})"
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

#[cold]
#[inline(never)]
fn length_mismatch_err(bitlen: i64, atttypmod: i32) -> PgError {
    PgError::error(format!(
        "bit string length {bitlen} does not match type bit({atttypmod})"
    ))
    .with_sqlstate(ERRCODE_STRING_DATA_LENGTH_MISMATCH)
}

#[cold]
#[inline(never)]
fn too_long_for_varying_err(atttypmod: i32) -> PgError {
    PgError::error(format!("bit string too long for type bit varying({atttypmod})"))
        .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
}

#[cold]
#[inline(never)]
fn bad_digit_err(c: u8, binary: bool) -> PgError {
    let kind = if binary { "binary" } else { "hexadecimal" };
    PgError::error(format!("\"{}\" is not a valid {kind} digit", c as char))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

/// C `DirectFunctionCall3(bit_in, string, InvalidOid, -1)` for the parser's
/// bit-string literal; hard errors only.
pub fn bit_in_cstr<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    Ok(bits_in(mcx, s, -1, true, None)?.expect("hard-error path returns Err"))
}

fn fc_bits_in(
    fcinfo: &mut Fcinfo,
    fixed: bool,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let atttypmod = fcinfo.arg(2).as_i32();
    let mcx = fcinfo.result_mcx();
    // SAFETY: context, if set, rides per the ErrorSaveNode contract.
    let esc = unsafe { fcinfo.soft_error_context() };
    match bits_in(mcx, s, atttypmod, fixed, esc)? {
        Some(img) => Ok(Datum::from_usize(img.leak().as_ptr() as usize)),
        None => Ok(Datum::null()),
    }
}

pub fn fc_bit_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bits_in(fcinfo, true)
}

pub fn fc_varbit_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bits_in(fcinfo, false)
}

fn fc_bits_out(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(cstring_result(bits_out(mcx, v.data())?))
}

pub fn fc_bit_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bits_out(fcinfo)
}

pub fn fc_varbit_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bits_out(fcinfo)
}


// varbit.c anybit_typmodin/out: typmod is the raw bit length (no VARHDRSZ).
fn anybit_typmodin(tl: &[i32], typename: &str) -> PgResult<i32> {
    if tl.len() != 1 {
        return Err(Box::new(
            PgError::error("invalid type modifier")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if tl[0] < 1 {
        return Err(Box::new(
            PgError::error(format!("length for type {typename} must be at least 1"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    // MaxAttrSize * BITS_PER_BYTE (htup_details.h).
    const MAX_BITS: i32 = 10 * 1024 * 1024 * 8;
    if tl[0] > MAX_BITS {
        return Err(Box::new(
            PgError::error(format!("length for type {typename} cannot exceed {MAX_BITS}"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    Ok(tl[0])
}

fn arg_typmod_array(fcinfo: &Fcinfo) -> &[u8] {
    // SAFETY: strict fn; arg 0 is a non-null cstring[] varlena image.
    unsafe {
        let p = fcinfo.arg_ptr(0);
        core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p))
    }
}

fn fc_bit_typmodin(fcinfo: &mut Fcinfo, typename: &str) -> PgResult<Datum> {
    let arr = arg_typmod_array(fcinfo);
    let mcx = fcinfo.result_mcx();
    let tl = ::arrayfuncs::construct::array_get_integer_typmods(mcx, arr)?;
    Ok(Datum::from_i32(anybit_typmodin(&tl, typename)?))
}

pub fn fc_bittypmodin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bit_typmodin(fcinfo, "bit")
}

pub fn fc_varbittypmodin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bit_typmodin(fcinfo, "bit varying")
}

fn fc_bit_typmodout(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg(0).as_i32();
    let mcx = fcinfo.result_mcx();
    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, 16)?;
    if typmod >= 0 {
        ::mcx::vec_append_bytes(&mut out, format!("({typmod})").as_bytes())?;
    }
    ::mcx::vec_append_bytes(&mut out, &[0])?;
    Ok(cstring_result(out))
}

pub fn fc_bittypmodout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bit_typmodout(fcinfo)
}

pub fn fc_varbittypmodout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bit_typmodout(fcinfo)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const VARBIT_BUILTINS: &[FmgrBuiltin] = &[
    b(1564, "bit_in", 3, fc_bit_in),
    b(1565, "bit_out", 1, fc_bit_out),
    b(1579, "varbit_in", 3, fc_varbit_in),
    b(1580, "varbit_out", 1, fc_varbit_out),
    b(2902, "varbittypmodin", 1, fc_varbittypmodin),
    b(2919, "bittypmodin", 1, fc_bittypmodin),
    b(2920, "bittypmodout", 1, fc_bittypmodout),
    b(2921, "varbittypmodout", 1, fc_varbittypmodout),
];
