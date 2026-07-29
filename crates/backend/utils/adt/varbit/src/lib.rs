//! varbit.c: I/O, recv/send, comparisons, logical ops, shifts, concat,
//! substring/overlay, int4/int8 casts, set/get bit, position, lengths,
//! typmod I/O and the bit()/varbit() length coercions + varbit_support.
#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::boxed::Box;
use alloc::format;

use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::stringinfo::StringInfo;
use ::types_core::Oid;
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_ARRAY_SUBSCRIPT_ERROR,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_STRING_DATA_LENGTH_MISMATCH,
    ERRCODE_STRING_DATA_RIGHT_TRUNCATION, ERRCODE_SUBSTRING_ERROR,
};
use ::types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
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
// Pub for proofs/varbit-rows (Kani C-equivalence harness; visibility only).
pub fn bits_in<'mcx>(
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
// Pub for proofs/varbit-rows (Kani C-equivalence harness; visibility only).
pub fn anybit_typmodin(tl: &[i32], typename: &str) -> PgResult<i32> {
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
    fc_bit_typmodin(fcinfo, "varbit")
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


// varbit.c bitfromint4: int4 -> bit(typmod), sign-filled, MSB-first.
pub fn bitfromint4_core<'mcx>(mcx: Mcx<'mcx>, a: i32, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    let typmod = if typmod <= 0 || typmod as i64 > VARBITMAXLEN { 1 } else { typmod };
    let nbits = typmod as usize;
    let len = varbit_total_len(nbits);
    let mut out: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, len)?;
    out.extend_from_slice(&::datum::varlena::set_varsize_4b(len));
    out.extend_from_slice(&(typmod).to_ne_bytes());
    for _ in (VARHDRSZ + VARBITHDRSZ)..len {
        out.push(0);
    }
    let r = &mut out[VARHDRSZ + VARBITHDRSZ..];
    let mut ri = 0usize;
    let mut destbitsleft = typmod;
    let srcbitsleft = 32i32.min(destbitsleft);
    while destbitsleft >= srcbitsleft + 8 {
        r[ri] = if a < 0 { 0xff } else { 0 };
        ri += 1;
        destbitsleft -= 8;
    }
    if destbitsleft > srcbitsleft {
        let mut val = (a >> (destbitsleft - 8)) as u32;
        if a < 0 {
            val |= (!0u32) << (srcbitsleft + 8 - destbitsleft);
        }
        r[ri] = (val & 0xff) as u8;
        ri += 1;
        destbitsleft -= 8;
    }
    while destbitsleft >= 8 {
        r[ri] = ((a >> (destbitsleft - 8)) & 0xff) as u8;
        ri += 1;
        destbitsleft -= 8;
    }
    if destbitsleft > 0 {
        r[ri] = ((a << (8 - destbitsleft)) & 0xff) as u8;
    }
    Ok(out)
}

pub fn fc_bitfromint4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = fcinfo.arg(0).as_i32();
    let typmod = fcinfo.arg(1).as_i32();
    let mcx = fcinfo.result_mcx();
    let img = bitfromint4_core(mcx, a, typmod)?;
    Ok(Datum::from_usize(img.leak().as_ptr() as usize))
}


// varbit.c bitfromint8: same fill as bitfromint4 with a 64-bit source.
pub fn bitfromint8_core<'mcx>(mcx: Mcx<'mcx>, a: i64, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    let typmod = if typmod <= 0 || typmod as i64 > VARBITMAXLEN { 1 } else { typmod };
    let nbits = typmod as usize;
    let len = varbit_total_len(nbits);
    let mut out: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, len)?;
    out.extend_from_slice(&::datum::varlena::set_varsize_4b(len));
    out.extend_from_slice(&typmod.to_ne_bytes());
    for _ in (VARHDRSZ + VARBITHDRSZ)..len {
        out.push(0);
    }
    let r = &mut out[VARHDRSZ + VARBITHDRSZ..];
    let mut ri = 0usize;
    let mut destbitsleft = typmod;
    let srcbitsleft = 64i32.min(destbitsleft);
    while destbitsleft >= srcbitsleft + 8 {
        r[ri] = if a < 0 { 0xff } else { 0 };
        ri += 1;
        destbitsleft -= 8;
    }
    if destbitsleft > srcbitsleft {
        let mut val = (a >> (destbitsleft - 8)) as u64;
        if a < 0 {
            val |= (!0u64) << (srcbitsleft + 8 - destbitsleft);
        }
        r[ri] = (val & 0xff) as u8;
        ri += 1;
        destbitsleft -= 8;
    }
    while destbitsleft >= 8 {
        r[ri] = ((a >> (destbitsleft - 8)) & 0xff) as u8;
        ri += 1;
        destbitsleft -= 8;
    }
    if destbitsleft > 0 {
        r[ri] = ((a << (8 - destbitsleft)) & 0xff) as u8;
    }
    Ok(out)
}

pub fn fc_bitfromint8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = fcinfo.arg(0).as_i64();
    let typmod = fcinfo.arg(1).as_i32();
    let mcx = fcinfo.result_mcx();
    let img = bitfromint8_core(mcx, a, typmod)?;
    Ok(Datum::from_usize(img.leak().as_ptr() as usize))
}

// varbit.c bit_cmp: byte memcmp then length. Pub for btree_gist's
// leaf-level bit comparisons.
pub fn bit_cmp_payload(a: &[u8], b: &[u8]) -> i32 {
    let (abits, abytes) = (i32::from_ne_bytes(a[..4].try_into().unwrap()), &a[4..]);
    let (bbits, bbytes) = (i32::from_ne_bytes(b[..4].try_into().unwrap()), &b[4..]);
    let n = abytes.len().min(bbytes.len());
    match abytes[..n].cmp(&bbytes[..n]) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Greater => 1,
        core::cmp::Ordering::Equal => {
            if abits != bbits {
                if abits < bbits { -1 } else { 1 }
            } else {
                0
            }
        }
    }
}

fn fc_bit_cmp_body(fcinfo: &mut Fcinfo, test: fn(i32) -> Datum) -> PgResult<Datum> {
    // SAFETY: strict fn; args 0/1 are non-null varbit varlenas.
    let a = unsafe { fcinfo.arg_varlena_packed(0)? };
    let b = unsafe { fcinfo.arg_varlena_packed(1)? };
    Ok(test(bit_cmp_payload(a.data(), b.data())))
}

macro_rules! bit_cmp_fns {
    ($($fc:ident $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            fc_bit_cmp_body(fcinfo, |c| Datum::from_bool(c $op 0))
        }
    )*};
}

bit_cmp_fns! {
    fc_biteq ==;
    fc_bitne !=;
    fc_bitlt <;
    fc_bitle <=;
    fc_bitgt >;
    fc_bitge >=;
}

pub fn fc_bitcmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bit_cmp_body(fcinfo, Datum::from_i32)
}

const HDRSZ: usize = VARHDRSZ + VARBITHDRSZ;
const BITMASK: u8 = 0xff;

// `payload` below is always the varlena body: [bit_len i32][zero-padded bits].
// pub for proofs/varbit-rows (bitlength/bitoctetlength value cores).
pub fn payload_bitlen(p: &[u8]) -> usize {
    i32::from_ne_bytes(p[..VARBITHDRSZ].try_into().unwrap()) as usize
}

// pub for proofs/varbit-rows (bitlength/bitoctetlength value cores).
pub fn payload_bits(p: &[u8]) -> &[u8] {
    &p[VARBITHDRSZ..]
}

// Full image [varsize][bitlen][zeroed bits]; resize never reallocates (capacity
// reserved fallibly up front).
fn varbit_alloc<'mcx>(mcx: Mcx<'mcx>, bitlen: usize) -> PgResult<PgVec<'mcx, u8>> {
    let len = varbit_total_len(bitlen);
    let mut out: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, len)?;
    out.extend_from_slice(&::datum::varlena::set_varsize_4b(len));
    out.extend_from_slice(&(bitlen as i32).to_ne_bytes());
    out.resize(len, 0);
    Ok(out)
}

// VARBIT_PAD: zero the pad bits of the last byte.
// Pub for proofs/bytea-varbit (Kani C-equivalence harness).
pub fn pad_last(body: &mut [u8], bitlen: usize) {
    let pad = body.len() * BITS_PER_BYTE - bitlen;
    if pad > 0 {
        let i = body.len() - 1;
        body[i] &= BITMASK << pad;
    }
}

fn image_datum(img: PgVec<'_, u8>) -> Datum {
    Datum::from_usize(img.leak().as_ptr() as usize)
}

// Pub for proofs/varbit-rows (Kani stubs the format! message plumbing).
#[cold]
#[inline(never)]
pub fn size_mismatch_err(opname: &'static str) -> PgError {
    PgError::error(format!("cannot {opname} bit strings of different sizes"))
        .with_sqlstate(ERRCODE_STRING_DATA_LENGTH_MISMATCH)
}

#[cold]
#[inline(never)]
fn negative_substring_err() -> PgError {
    PgError::error("negative substring length not allowed")
        .with_sqlstate(ERRCODE_SUBSTRING_ERROR)
}

#[cold]
#[inline(never)]
// pub for proofs/varbit-rows (bittoint4/8 error-arm stub target).
pub fn out_of_range_err(what: &'static str) -> PgError {
    PgError::error(format!("{what} out of range")).with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
fn invalid_external_len_err() -> PgError {
    PgError::error("invalid length in external bit string")
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION)
}

#[cold]
#[inline(never)]
fn bit_index_err(n: i32, bitlen: usize) -> PgError {
    PgError::error(format!(
        "bit index {n} out of valid range (0..{})",
        bitlen as i64 - 1
    ))
    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
}

#[cold]
#[inline(never)]
fn new_bit_err() -> PgError {
    PgError::error("new bit must be 0 or 1").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

// Length-mismatch verdict of bit_logic. Pub slice core for proofs/varbit-rows
// (Kani C-equivalence harness); factored out of the Mcx-bound bit_logic.
pub fn bit_logic_verdict(a: &[u8], b: &[u8], opname: &'static str) -> PgResult<usize> {
    let bitlen1 = payload_bitlen(a);
    if bitlen1 != payload_bitlen(b) {
        return Err(size_mismatch_err(opname).into());
    }
    Ok(bitlen1)
}

// Byte-combine body of bit_logic; `r` is the output payload bits (zeroed).
// Pub slice core for proofs/varbit-rows (Kani C-equivalence harness).
pub fn bit_logic_body(r: &mut [u8], b1: &[u8], b2: &[u8], op: fn(u8, u8) -> u8) {
    for ((r, &p1), &p2) in r.iter_mut().zip(b1).zip(b2) {
        *r = op(p1, p2);
    }
}

pub fn bit_logic<'mcx>(
    mcx: Mcx<'mcx>,
    a: &[u8],
    b: &[u8],
    op: fn(u8, u8) -> u8,
    opname: &'static str,
) -> PgResult<PgVec<'mcx, u8>> {
    let bitlen1 = bit_logic_verdict(a, b, opname)?;
    let mut out = varbit_alloc(mcx, bitlen1)?;
    bit_logic_body(&mut out[HDRSZ..], payload_bits(a), payload_bits(b), op);
    Ok(out)
}

fn fc_bit_logic(
    fcinfo: &mut Fcinfo,
    op: fn(u8, u8) -> u8,
    opname: &'static str,
) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null varbit varlenas (strict fn).
    let a = unsafe { fcinfo.arg_varlena_packed(0)? };
    let b = unsafe { fcinfo.arg_varlena_packed(1)? };
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bit_logic(mcx, a.data(), b.data(), op, opname)?))
}

pub fn fc_bit_and(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bit_logic(fcinfo, |a, b| a & b, "AND")
}

pub fn fc_bit_or(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bit_logic(fcinfo, |a, b| a | b, "OR")
}

pub fn fc_bitxor(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bit_logic(fcinfo, |a, b| a ^ b, "XOR")
}

// Byte-invert + repad body of bitnot; `r` is the output payload bits.
// Pub slice core for proofs/varbit-rows (Kani C-equivalence harness).
pub fn bitnot_body(r: &mut [u8], bits: &[u8], bitlen: usize) {
    for (r, &b) in r.iter_mut().zip(bits) {
        *r = !b;
    }
    pad_last(r, bitlen);
}

pub fn bitnot_core<'mcx>(mcx: Mcx<'mcx>, p: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let bitlen = payload_bitlen(p);
    let mut out = varbit_alloc(mcx, bitlen)?;
    bitnot_body(&mut out[HDRSZ..], payload_bits(p), bitlen);
    Ok(out)
}

pub fn fc_bitnot(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bitnot_core(mcx, v.data())?))
}

// Shift bodies: `r` is the output payload bits, zeroed, r.len() == bits.len().
// Pub slice cores for proofs/varbit-rows (Kani C-equivalence harness);
// factored out of the Mcx-bound *_core fns (incl. the negative-shift
// dispatch, which recursed across the cores before allocation — both
// directions allocate the identical zeroed image, so hoisting the alloc
// above the dispatch is behavior-preserving).
pub fn bitshiftleft_body(r: &mut [u8], bits: &[u8], bitlen: usize, shft: i32) {
    if shft < 0 {
        // The clamped negation is always >= 0, so the cross-call can never
        // recurse further; calling the positive core directly (rather than
        // the sibling dispatcher) keeps the call graph acyclic — behavior
        // identical, and Kani/CBMC would otherwise unwind the syntactic
        // left<->right recursion cycle to the loop bound (~10x formula,
        // measured 40s vs 2s in proofs/varbit-rows).
        let shft = shft.max(-(VARBITMAXLEN as i32));
        return bitshiftright_pos(r, bits, bitlen, -shft);
    }
    bitshiftleft_pos(r, bits, bitlen, shft);
}

// Non-negative-shift core of bitshiftleft_body (proofs/varbit-rows).
fn bitshiftleft_pos(r: &mut [u8], bits: &[u8], bitlen: usize, shft: i32) {
    if shft as usize >= bitlen {
        return;
    }
    let byte_shift = shft as usize / BITS_PER_BYTE;
    let ishift = shft as usize % BITS_PER_BYTE;
    if ishift == 0 {
        let len = bits.len() - byte_shift;
        r[..len].copy_from_slice(&bits[byte_shift..]);
    } else {
        for (ri, pi) in (byte_shift..bits.len()).enumerate() {
            r[ri] = bits[pi] << ishift;
            if pi + 1 < bits.len() {
                r[ri] |= bits[pi + 1] >> (BITS_PER_BYTE - ishift);
            }
        }
    }
}

pub fn bitshiftright_body(r: &mut [u8], bits: &[u8], bitlen: usize, shft: i32) {
    if shft < 0 {
        // See bitshiftleft_body: acyclic dispatch into the positive core.
        let shft = shft.max(-(VARBITMAXLEN as i32));
        return bitshiftleft_pos(r, bits, bitlen, -shft);
    }
    bitshiftright_pos(r, bits, bitlen, shft);
}

// Non-negative-shift core of bitshiftright_body (proofs/varbit-rows).
fn bitshiftright_pos(r: &mut [u8], bits: &[u8], bitlen: usize, shft: i32) {
    if shft as usize >= bitlen {
        return;
    }
    let byte_shift = shft as usize / BITS_PER_BYTE;
    let ishift = shft as usize % BITS_PER_BYTE;
    if ishift == 0 {
        let len = bits.len() - byte_shift;
        r[byte_shift..].copy_from_slice(&bits[..len]);
    } else {
        let mut ri = byte_shift;
        for &pb in bits {
            if ri >= r.len() {
                break;
            }
            r[ri] |= pb >> ishift;
            ri += 1;
            if ri < r.len() {
                r[ri] = pb << (BITS_PER_BYTE - ishift);
            }
        }
    }
    // C VARBIT_PAD_LAST: 1s can shift into the pad bits in either branch.
    pad_last(r, bitlen);
}

pub fn bitshiftleft_core<'mcx>(
    mcx: Mcx<'mcx>,
    p: &[u8],
    shft: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let bitlen = payload_bitlen(p);
    let mut out = varbit_alloc(mcx, bitlen)?;
    bitshiftleft_body(&mut out[HDRSZ..], payload_bits(p), bitlen, shft);
    Ok(out)
}

pub fn bitshiftright_core<'mcx>(
    mcx: Mcx<'mcx>,
    p: &[u8],
    shft: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let bitlen = payload_bitlen(p);
    let mut out = varbit_alloc(mcx, bitlen)?;
    bitshiftright_body(&mut out[HDRSZ..], payload_bits(p), bitlen, shft);
    Ok(out)
}

pub fn fc_bitshiftleft(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let shft = fcinfo.arg(1).as_i32();
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bitshiftleft_core(mcx, v.data(), shft)?))
}

pub fn fc_bitshiftright(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let shft = fcinfo.arg(1).as_i32();
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bitshiftright_core(mcx, v.data(), shft)?))
}

pub fn bit_catenate<'mcx>(mcx: Mcx<'mcx>, p1: &[u8], p2: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let bitlen1 = payload_bitlen(p1);
    let bitlen2 = payload_bitlen(p2);
    if bitlen1 as i64 > VARBITMAXLEN - bitlen2 as i64 {
        return Err(too_long_err().into());
    }
    let b1 = payload_bits(p1);
    let b2 = payload_bits(p2);
    let mut out = varbit_alloc(mcx, bitlen1 + bitlen2)?;
    let r = &mut out[HDRSZ..];
    r[..b1.len()].copy_from_slice(b1);
    let bit1pad = b1.len() * BITS_PER_BYTE - bitlen1;
    if bit1pad == 0 {
        r[b1.len()..b1.len() + b2.len()].copy_from_slice(b2);
    } else if bitlen2 > 0 {
        let bit2shift = BITS_PER_BYTE - bit1pad;
        let mut ri = b1.len() - 1;
        for &pa in b2 {
            r[ri] |= pa >> bit2shift;
            ri += 1;
            if ri < r.len() {
                r[ri] = pa << bit1pad;
            }
        }
    }
    Ok(out)
}

pub fn fc_bitcat(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null varbit varlenas (strict fn).
    let a = unsafe { fcinfo.arg_varlena_packed(0)? };
    let b = unsafe { fcinfo.arg_varlena_packed(1)? };
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bit_catenate(mcx, a.data(), b.data())?))
}

pub fn bitsubstring<'mcx>(
    mcx: Mcx<'mcx>,
    p: &[u8],
    s: i32,
    l: i32,
    length_not_specified: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let bitlen = payload_bitlen(p) as i32;
    let s1 = s.max(1);
    let e1 = if length_not_specified {
        bitlen + 1
    } else if l < 0 {
        return Err(negative_substring_err().into());
    } else {
        match s.checked_add(l) {
            // S + L overflow: substring runs to end of string.
            None => bitlen + 1,
            Some(e) => e.min(bitlen + 1),
        }
    };
    if s1 > bitlen || e1 <= s1 {
        return varbit_alloc(mcx, 0);
    }
    let rbitlen = (e1 - s1) as usize;
    let mut out = varbit_alloc(mcx, rbitlen)?;
    let bits = payload_bits(p);
    let r = &mut out[HDRSZ..];
    let start = (s1 - 1) as usize;
    let ps0 = start / BITS_PER_BYTE;
    let ishift = start % BITS_PER_BYTE;
    if ishift == 0 {
        r.copy_from_slice(&bits[ps0..ps0 + r.len()]);
    } else {
        for i in 0..r.len() {
            r[i] = bits[ps0 + i] << ishift;
            if ps0 + i + 1 < bits.len() {
                r[i] |= bits[ps0 + i + 1] >> (BITS_PER_BYTE - ishift);
            }
        }
    }
    pad_last(r, rbitlen);
    Ok(out)
}

pub fn fc_bitsubstr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let s = fcinfo.arg(1).as_i32();
    let l = fcinfo.arg(2).as_i32();
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bitsubstring(mcx, v.data(), s, l, false)?))
}

pub fn fc_bitsubstr_no_len(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let s = fcinfo.arg(1).as_i32();
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bitsubstring(mcx, v.data(), s, -1, true)?))
}

pub fn bit_overlay<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    sp: i32,
    sl: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    if sp <= 0 {
        return Err(negative_substring_err().into());
    }
    let Some(sp_pl_sl) = sp.checked_add(sl) else {
        return Err(out_of_range_err("integer").into());
    };
    let s1 = bitsubstring(mcx, t1, 1, sp - 1, false)?;
    let s2 = bitsubstring(mcx, t1, sp_pl_sl, -1, true)?;
    let head = bit_catenate(mcx, &s1[VARHDRSZ..], t2)?;
    bit_catenate(mcx, &head[VARHDRSZ..], &s2[VARHDRSZ..])
}

pub fn fc_bitoverlay(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null varbit varlenas (strict fn).
    let t1 = unsafe { fcinfo.arg_varlena_packed(0)? };
    let t2 = unsafe { fcinfo.arg_varlena_packed(1)? };
    let sp = fcinfo.arg(2).as_i32();
    let sl = fcinfo.arg(3).as_i32();
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bit_overlay(mcx, t1.data(), t2.data(), sp, sl)?))
}

pub fn fc_bitoverlay_no_len(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null varbit varlenas (strict fn).
    let t1 = unsafe { fcinfo.arg_varlena_packed(0)? };
    let t2 = unsafe { fcinfo.arg_varlena_packed(1)? };
    let sp = fcinfo.arg(2).as_i32();
    let sl = payload_bitlen(t2.data()) as i32;
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bit_overlay(mcx, t1.data(), t2.data(), sp, sl)?))
}

pub fn fc_bit_bit_count(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let n: i64 = payload_bits(v.data())
        .iter()
        .map(|&b| b.count_ones() as i64)
        .sum();
    Ok(Datum::from_i64(n))
}

pub fn fc_bitlength(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i32(payload_bitlen(v.data()) as i32))
}

pub fn fc_bitoctetlength(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i32(payload_bits(v.data()).len() as i32))
}

pub fn bittoint4_core(p: &[u8]) -> PgResult<i32> {
    let bitlen = payload_bitlen(p);
    if bitlen > 32 {
        return Err(out_of_range_err("integer").into());
    }
    let bits = payload_bits(p);
    let mut result: u32 = 0;
    for &b in bits {
        result <<= BITS_PER_BYTE;
        result |= b as u32;
    }
    result >>= bits.len() * BITS_PER_BYTE - bitlen;
    Ok(result as i32)
}

pub fn fc_bittoint4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i32(bittoint4_core(v.data())?))
}

pub fn bittoint8_core(p: &[u8]) -> PgResult<i64> {
    let bitlen = payload_bitlen(p);
    if bitlen > 64 {
        return Err(out_of_range_err("bigint").into());
    }
    let bits = payload_bits(p);
    let mut result: u64 = 0;
    for &b in bits {
        result <<= BITS_PER_BYTE;
        result |= b as u64;
    }
    result >>= bits.len() * BITS_PER_BYTE - bitlen;
    Ok(result as i64)
}

pub fn fc_bittoint8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i64(bittoint8_core(v.data())?))
}

pub fn bits_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    atttypmod: i32,
    fixed: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let bitlen = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    if bitlen < 0 || bitlen as i64 > VARBITMAXLEN {
        return Err(invalid_external_len_err().into());
    }
    if fixed {
        if atttypmod > 0 && bitlen != atttypmod {
            return Err(Box::new(length_mismatch_err(bitlen as i64, atttypmod)));
        }
    } else if atttypmod > 0 && bitlen > atttypmod {
        return Err(Box::new(too_long_for_varying_err(atttypmod)));
    }
    let mut out = varbit_alloc(mcx, bitlen as usize)?;
    let body = &mut out[HDRSZ..];
    ::pqformat::pq_copymsgbytes(buf, body)?;
    pad_last(body, bitlen as usize);
    Ok(out)
}

fn fc_bits_recv(fcinfo: &mut Fcinfo, fixed: bool) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let atttypmod = fcinfo.arg(2).as_i32();
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bits_recv(mcx, buf, atttypmod, fixed)?))
}

pub fn fc_bit_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bits_recv(fcinfo, true)
}

pub fn fc_varbit_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bits_recv(fcinfo, false)
}

fn fc_bits_send(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let p = v.data();
    let mcx = fcinfo.result_mcx();
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, payload_bitlen(p) as u32)?;
    ::pqformat::pq_sendbytes(&mut buf, payload_bits(p))?;
    Ok(varlena_result(::pqformat::pq_endtypsend(buf)))
}

pub fn fc_bit_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bits_send(fcinfo)
}

pub fn fc_varbit_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fc_bits_send(fcinfo)
}

// bit() length coercion. Ok(None) = return the source datum unchanged.
pub fn bit_coerce<'mcx>(
    mcx: Mcx<'mcx>,
    p: &[u8],
    len: i32,
    is_explicit: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let bitlen = payload_bitlen(p);
    if len <= 0 || len as i64 > VARBITMAXLEN || len as usize == bitlen {
        return Ok(None);
    }
    if !is_explicit {
        return Err(Box::new(length_mismatch_err(bitlen as i64, len)));
    }
    let mut out = varbit_alloc(mcx, len as usize)?;
    let r = &mut out[HDRSZ..];
    let bits = payload_bits(p);
    let n = r.len().min(bits.len());
    r[..n].copy_from_slice(&bits[..n]);
    pad_last(r, len as usize);
    Ok(Some(out))
}

pub fn fc_bit_coerce(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let len = fcinfo.arg(1).as_i32();
    let is_explicit = fcinfo.arg(2).as_bool();
    let mcx = fcinfo.result_mcx();
    match bit_coerce(mcx, v.data(), len, is_explicit)? {
        Some(img) => Ok(image_datum(img)),
        None => Ok(fcinfo.arg(0)),
    }
}

// varbit() length coercion. Ok(None) = return the source datum unchanged.
pub fn varbit_coerce<'mcx>(
    mcx: Mcx<'mcx>,
    p: &[u8],
    len: i32,
    is_explicit: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let bitlen = payload_bitlen(p);
    if len <= 0 || len as usize >= bitlen {
        return Ok(None);
    }
    if !is_explicit {
        return Err(Box::new(too_long_for_varying_err(len)));
    }
    let mut out = varbit_alloc(mcx, len as usize)?;
    let r = &mut out[HDRSZ..];
    let bits = payload_bits(p);
    let n = r.len();
    r.copy_from_slice(&bits[..n]);
    pad_last(r, len as usize);
    Ok(Some(out))
}

pub fn fc_varbit_coerce(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let len = fcinfo.arg(1).as_i32();
    let is_explicit = fcinfo.arg(2).as_bool();
    let mcx = fcinfo.result_mcx();
    match varbit_coerce(mcx, v.data(), len, is_explicit)? {
        Some(img) => Ok(image_datum(img)),
        None => Ok(fcinfo.arg(0)),
    }
}

// varbit_support (varbit.c): SupportRequestSimplify only — widening (or
// unconstraining) a varbit typmod becomes a RelabelType, no rewrite.
pub fn fc_varbit_support(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use ::types_nodes::{supportnodes::SupportRequestSimplify, NodeTag};
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *const NodeTag;
    // SAFETY: prosupport contract — arg points at a live tag-first node.
    if unsafe { *p } != NodeTag::T_SupportRequestSimplify {
        return Ok(Datum::from_usize(0));
    }
    // SAFETY: tag checked; the planner owns the request node for the call.
    let req = unsafe { &*(a.value.as_usize() as *const SupportRequestSimplify) };
    let fexpr = req
        .fcall
        .and_then(|n| n.as_func_expr())
        .unwrap_or_else(|| panic!("varbit_support: SupportRequestSimplify without a FuncExpr"));
    assert!(fexpr.args.len() >= 2);
    let Some(c) = fexpr.args.nth(1).as_const() else {
        return Ok(Datum::from_usize(0));
    };
    if c.constisnull {
        return Ok(Datum::from_usize(0));
    }
    let source = fexpr.args.nth(0);
    let old_max = ::nodes_core::expr_typmod(source);
    let new_typmod = c.constvalue.as_i32();
    let new_max = new_typmod;
    // C: varbit() treats typmod 0 as invalid, so simplify that case too.
    if new_max <= 0 || (old_max > 0 && old_max <= new_max) {
        let mcx = req.mcx.expect("varbit_support: request carries an mcx");
        let ret = ::nodes_core::relabel_to_typmod(mcx, source, new_typmod)?;
        return Ok(Datum::from_usize(ret.as_raw().as_ptr() as usize));
    }
    Ok(Datum::from_usize(0))
}

pub fn bitposition_core(str_p: &[u8], substr_p: &[u8]) -> i32 {
    let substr_length = payload_bitlen(substr_p);
    let str_length = payload_bitlen(str_p);
    if str_length == 0 || substr_length > str_length {
        return 0;
    }
    if substr_length == 0 {
        return 1;
    }
    let sb = payload_bits(str_p);
    let pb = payload_bits(substr_p);
    let shl = |b: u8, n: usize| -> u8 { ((b as u32) << n) as u8 };
    let end_mask = shl(BITMASK, pb.len() * BITS_PER_BYTE - substr_length);
    let str_mask = shl(BITMASK, sb.len() * BITS_PER_BYTE - str_length);
    for i in 0..=(sb.len() - pb.len()) {
        for is in 0..BITS_PER_BYTE {
            let mut is_match;
            let mut pi = i;
            let mut mask1 = BITMASK >> is;
            let mut mask2 = !mask1;
            let mut s = 0;
            loop {
                let mut cmp = pb[s] >> is;
                if s == pb.len() - 1 {
                    mask1 &= end_mask >> is;
                    if pi == sb.len() - 1 {
                        if mask1 & !str_mask != 0 {
                            is_match = false;
                            break;
                        }
                        mask1 &= str_mask;
                    }
                }
                is_match = (cmp ^ sb[pi]) & mask1 == 0;
                if !is_match {
                    break;
                }
                pi += 1;
                if pi == sb.len() {
                    mask2 = shl(end_mask, BITS_PER_BYTE - is);
                    is_match = mask2 == 0;
                    break;
                }
                cmp = shl(pb[s], BITS_PER_BYTE - is);
                if s == pb.len() - 1 {
                    mask2 &= shl(end_mask, BITS_PER_BYTE - is);
                    if pi == sb.len() - 1 {
                        if mask2 & !str_mask != 0 {
                            is_match = false;
                            break;
                        }
                        mask2 &= str_mask;
                    }
                }
                is_match = (cmp ^ sb[pi]) & mask2 == 0;
                s += 1;
                if !(is_match && s < pb.len()) {
                    break;
                }
            }
            if is_match {
                return (i * BITS_PER_BYTE + is + 1) as i32;
            }
        }
    }
    0
}

pub fn fc_bitposition(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args 0/1 are non-null varbit varlenas (strict fn).
    let s = unsafe { fcinfo.arg_varlena_packed(0)? };
    let sub = unsafe { fcinfo.arg_varlena_packed(1)? };
    Ok(Datum::from_i32(bitposition_core(s.data(), sub.data())))
}

pub fn bitsetbit_core<'mcx>(
    mcx: Mcx<'mcx>,
    p: &[u8],
    n: i32,
    new_bit: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let bitlen = payload_bitlen(p);
    if n < 0 || n as usize >= bitlen {
        return Err(bit_index_err(n, bitlen).into());
    }
    if new_bit != 0 && new_bit != 1 {
        return Err(new_bit_err().into());
    }
    let mut out = varbit_alloc(mcx, bitlen)?;
    let r = &mut out[HDRSZ..];
    r.copy_from_slice(payload_bits(p));
    let byte_no = n as usize / BITS_PER_BYTE;
    let bit_no = BITS_PER_BYTE - 1 - (n as usize % BITS_PER_BYTE);
    if new_bit == 0 {
        r[byte_no] &= !(1 << bit_no);
    } else {
        r[byte_no] |= 1 << bit_no;
    }
    Ok(out)
}

pub fn fc_bitsetbit(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let n = fcinfo.arg(1).as_i32();
    let new_bit = fcinfo.arg(2).as_i32();
    let mcx = fcinfo.result_mcx();
    Ok(image_datum(bitsetbit_core(mcx, v.data(), n, new_bit)?))
}

pub fn bitgetbit_core(p: &[u8], n: i32) -> PgResult<i32> {
    let bitlen = payload_bitlen(p);
    if n < 0 || n as usize >= bitlen {
        return Err(bit_index_err(n, bitlen).into());
    }
    let byte_no = n as usize / BITS_PER_BYTE;
    let bit_no = BITS_PER_BYTE - 1 - (n as usize % BITS_PER_BYTE);
    Ok(((payload_bits(p)[byte_no] >> bit_no) & 1) as i32)
}

pub fn fc_bitgetbit(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null varbit varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let n = fcinfo.arg(1).as_i32();
    Ok(Datum::from_i32(bitgetbit_core(v.data(), n)?))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const VARBIT_BUILTINS: &[FmgrBuiltin] = &[
    b(1564, "bit_in", 3, fc_bit_in),
    b(1565, "bit_out", 1, fc_bit_out),
    b(1579, "varbit_in", 3, fc_varbit_in),
    b(1580, "varbit_out", 1, fc_varbit_out),
    b(1581, "biteq", 2, fc_biteq),
    b(1582, "bitne", 2, fc_bitne),
    b(1592, "bitge", 2, fc_bitge),
    b(1593, "bitgt", 2, fc_bitgt),
    b(1594, "bitle", 2, fc_bitle),
    b(1595, "bitlt", 2, fc_bitlt),
    b(1596, "bitcmp", 2, fc_bitcmp),
    // 1666-1672: varbiteq..varbitcmp pg_proc aliases, prosrc = the bit fns.
    b(1666, "biteq", 2, fc_biteq),
    b(1667, "bitne", 2, fc_bitne),
    b(1668, "bitge", 2, fc_bitge),
    b(1669, "bitgt", 2, fc_bitgt),
    b(1670, "bitle", 2, fc_bitle),
    b(1671, "bitlt", 2, fc_bitlt),
    b(1672, "bitcmp", 2, fc_bitcmp),
    b(1673, "bit_and", 2, fc_bit_and),
    b(1674, "bit_or", 2, fc_bit_or),
    b(1675, "bitxor", 2, fc_bitxor),
    b(1676, "bitnot", 1, fc_bitnot),
    b(1677, "bitshiftleft", 2, fc_bitshiftleft),
    b(1678, "bitshiftright", 2, fc_bitshiftright),
    b(1679, "bitcat", 2, fc_bitcat),
    b(1680, "bitsubstr", 3, fc_bitsubstr),
    b(1681, "bitlength", 1, fc_bitlength),
    b(1682, "bitoctetlength", 1, fc_bitoctetlength),
    b(1683, "bitfromint4", 2, fc_bitfromint4),
    b(1684, "bittoint4", 1, fc_bittoint4),
    b(1685, "bit", 3, fc_bit_coerce),
    b(1687, "varbit", 3, fc_varbit_coerce),
    b(1698, "bitposition", 2, fc_bitposition),
    b(1699, "bitsubstr_no_len", 2, fc_bitsubstr_no_len),
    b(2075, "bitfromint8", 2, fc_bitfromint8),
    b(2076, "bittoint8", 1, fc_bittoint8),
    b(2456, "bit_recv", 3, fc_bit_recv),
    b(2457, "bit_send", 1, fc_bit_send),
    b(2458, "varbit_recv", 3, fc_varbit_recv),
    b(2459, "varbit_send", 1, fc_varbit_send),
    b(2902, "varbittypmodin", 1, fc_varbittypmodin),
    b(2919, "bittypmodin", 1, fc_bittypmodin),
    b(2920, "bittypmodout", 1, fc_bittypmodout),
    b(2921, "varbittypmodout", 1, fc_varbittypmodout),
    b(3030, "bitoverlay", 4, fc_bitoverlay),
    b(3031, "bitoverlay_no_len", 3, fc_bitoverlay_no_len),
    b(3032, "bitgetbit", 2, fc_bitgetbit),
    b(3033, "bitsetbit", 3, fc_bitsetbit),
    b(3158, "varbit_support", 1, fc_varbit_support),
    b(6162, "bit_bit_count", 1, fc_bit_bit_count),
];

#[cfg(test)]
mod tests;
