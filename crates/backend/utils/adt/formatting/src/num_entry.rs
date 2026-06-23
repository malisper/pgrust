//! NUM (number) SQL entry-point cores: `numeric_to_number`, `numeric_to_char`,
//! `int4_to_char`, `int8_to_char`, `float4_to_char`, `float8_to_char`.
//!
//! Faithful idiomatic port of formatting.c:6289-6937 (PG 18.3). These operate
//! on Rust vocabulary (`NumericVar` / `i32` / `i64` / `f32` / `f64` + a format
//! byte slice) and return owned text bytes (`to_char`) or a `NumericVar`
//! (`to_number`); the fmgr `Datum` boundary is the seamed caller boundary
//! (project systemic deferral).
//!
//! ## Seams
//!
//! The `NumericVar` arithmetic lives in the `backend-utils-adt-numeric` sibling
//! crate (a separate port) and routes through the centralized
//! `seams::formatting` slots: `numeric_in_var`,
//! `numeric_out_var`, `numeric_out_sci`, `numeric_round`, `apply_typmod`,
//! `mul_var`, `power_var`, `int8_numeric`, `numericvar_to_int32`,
//! `make_numeric_typmod`. The format-picture engine driver (`num_processor`)
//! and the to_char finishing/padding logic are ported in-crate (see
//! [`crate::num`]).

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_numeric::var::NumericVar;
use ::types_core::Oid;

use crate::num::{fill_str, int_to_roman, num_processor};
use crate::printf::{fmt_f, fmt_f0, fmt_plus_e};
use crate::tables::*;

// ---------------------------------------------------------------------------
// NumericVar arithmetic — direct calls into the backend-utils-adt-numeric
// sibling crate (a DIRECT dep). Every NumericVar-producing routine carries the
// `'mcx` lifetime of the memory context that owns its digit buffer, so `mcx`
// threads through every function that builds or transforms a NumericVar.
//
// `VARHDRSZ` — the uncompressed varlena length-word size, in bytes.
const VARHDRSZ: i32 = 4;

/// C: `make_numeric_typmod(precision, scale)` (numeric.c) — there is no
/// exported helper in the numeric crate, so we mirror it inline:
/// `((precision << 16) | scale) + VARHDRSZ`.
fn make_numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ
}

// ---------------------------------------------------------------------------
// Local helpers mirroring C macros / inline routines.
// ---------------------------------------------------------------------------

/// Truncate a NUL-terminated working buffer at the first NUL (C strings the
/// processor leaves).
fn cstr(b: &[u8]) -> Vec<u8> {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    b[..end].to_vec()
}

/// C: `NUM_cache` (formatting.c:5017) — get the parsed format + a copy of the
/// prepared `NUMDesc` for the picture `fmt`. In this port the cache always owns
/// a copy, so `shouldFree` is irrelevant; we just return the pair.
fn num_cache(len: usize, fmt: &[u8]) -> PgResult<(Vec<FormatNode>, NUMDesc)> {
    if len > NUM_CACHE_SIZE {
        // Bigger than the cache: parse directly with a fresh NUMDesc.
        let mut num = NUMDesc::default();
        num.zeroize();
        let format = crate::parse::parse_format(
            fmt,
            NUM_KEYWORDS,
            &[],
            &NUM_INDEX,
            NUM_FLAG,
            Some(&mut num),
        )?;
        Ok((format, num))
    } else {
        crate::cache::num_cache_fetch(fmt)
    }
}

/// Build the working `numstr` and run `NUM_processor` for the to_char path.
/// C: `NUM_TOCHAR_finish` (formatting.c:6302). `result_cap` is `len *
/// NUM_MAX_ITEM_SIZ` (the VARDATA workspace size).
fn num_tochar_finish(
    format: &[FormatNode],
    num: &mut NUMDesc,
    numstr: Vec<u8>,
    out_pre_spaces: i32,
    sign: i32,
    fmt_len: usize,
    collid: Oid,
) -> PgResult<Vec<u8>> {
    // NUM_processor's working buffers stay `Vec<u8>` scratch (mirroring the C
    // `char *` palloc), so no `mcx` thread-through is needed here.
    let inout = vec![0u8; fmt_len * NUM_MAX_ITEM_SIZ + 1];
    let processed = num_processor(
        format,
        num,
        inout,
        numstr,
        0,
        out_pre_spaces,
        sign,
        true,
        collid,
    )?;
    Ok(processed.out)
}

// ---------------------------------------------------------------------------
// to_number.
// ---------------------------------------------------------------------------

/// C: `numeric_to_number` (formatting.c:6324) core. Returns the parsed
/// `NumericVar` (the caller applies the typmod / wraps as a Datum), or `None`
/// for the SQL-NULL case (empty/oversized fmt).
pub fn numeric_to_number<'mcx>(
    mcx: Mcx<'mcx>,
    value: &[u8],
    fmt: &[u8],
    collid: Oid,
) -> PgResult<Option<NumericVar<'mcx>>> {
    let len = fmt.len();
    if len == 0 || len >= (i32::MAX as usize) / NUM_MAX_ITEM_SIZ {
        return Ok(None);
    }

    let (format, mut num) = num_cache(len, fmt)?;

    let numstr = vec![0u8; len * NUM_MAX_ITEM_SIZ + 1];
    let processed = num_processor(
        &format,
        &mut num,
        value.to_vec(),
        numstr,
        value.len(),
        0,
        0,
        false,
        collid,
    )?;

    let scale = num.post;
    let precision = num.pre + num.multi + scale;

    // C: DirectFunctionCall3(numeric_in, numstr, InvalidOid,
    //        ((precision << 16) | scale) + VARHDRSZ)
    // numeric_in both parses the value (skipping the leading sign-space the
    // processor left) AND applies the typmod (round to `scale` + enforce the
    // precision limit). This repo's `numeric_in(mcx, s, typmod)` does exactly
    // that, returning the packed on-disk image; `set_var_from_num` then decodes
    // it back into a NumericVar for the IS_MULTI arithmetic below (mirroring C's
    // DatumGetNumeric on the numeric_in result). The fmgr context here is NULL
    // (DirectFunctionCall), so any malformed-input or typmod error is hard.
    let s = String::from_utf8_lossy(&cstr(&processed.out)).into_owned();
    let packed = adt_numeric::io::numeric_in(
        mcx,
        &s,
        make_numeric_typmod(precision, scale),
    )?;
    let mut var = adt_numeric::convert::set_var_from_num(mcx, &packed)?;

    if num.is_multi() {
        // result *= 10^(-multi)
        let a = adt_numeric::kernel_transcendental::int64_to_numericvar(mcx, 10)?;
        let b = adt_numeric::kernel_transcendental::int64_to_numericvar(
            mcx,
            -(num.multi as i64),
        )?;
        let x = adt_numeric::kernel_transcendental::power_var(mcx, &a, &b)?;
        let rscale = select_mul_rscale(&var, &x);
        var = adt_numeric::kernel_var::mul_var(mcx, &var, &x, rscale)?;
    }

    Ok(Some(var))
}

// ---------------------------------------------------------------------------
// to_char producers.
// ---------------------------------------------------------------------------

/// C: `numeric_to_char` (formatting.c:6383) core. `value` is the input
/// `NumericVar`. Returns the formatted text bytes.
pub fn numeric_to_char<'mcx>(
    mcx: Mcx<'mcx>,
    value: &NumericVar<'mcx>,
    fmt: &[u8],
    collid: Oid,
) -> PgResult<Vec<u8>> {
    let len = fmt.len();
    if len == 0 || len >= (i32::MAX as usize - 4) / NUM_MAX_ITEM_SIZ {
        return Ok(Vec::new()); // PG_RETURN_TEXT_P(cstring_to_text(""))
    }
    let (format, mut num) = num_cache(len, fmt)?;

    let mut out_pre_spaces = 0i32;
    let mut sign = 0i32;
    let numstr: Vec<u8>;

    if num.is_roman() {
        // numeric_int4_opt_error: overflow -> PG_INT32_MAX.
        let intvalue = numericvar_to_int4_opt(value)?.unwrap_or(i32::MAX);
        numstr = int_to_roman(intvalue);
    } else if num.is_eeee() {
        let orgnum = numeric_out_sci_var(value, num.post)?;
        if orgnum == "NaN" || orgnum == "Infinity" || orgnum == "-Infinity" {
            let mut ns = fill_str(b'#', (num.pre + num.post + 6) as usize);
            ns[0] = b' ';
            // *(numstr + Num.pre + 1) = '.'
            let dot = (num.pre + 1) as usize;
            if dot < ns.len() {
                ns[dot] = b'.';
            }
            numstr = ns;
        } else if !orgnum.starts_with('-') {
            let mut ns = Vec::with_capacity(orgnum.len() + 1);
            ns.push(b' ');
            ns.extend_from_slice(orgnum.as_bytes());
            numstr = ns;
        } else {
            numstr = orgnum.into_bytes();
        }
    } else {
        let mut val = value.clone();
        if num.is_multi() {
            let a = adt_numeric::kernel_transcendental::int64_to_numericvar(mcx, 10)?;
            let b = adt_numeric::kernel_transcendental::int64_to_numericvar(
                mcx,
                num.multi as i64,
            )?;
            let x = adt_numeric::kernel_transcendental::power_var(mcx, &a, &b)?;
            let rscale = select_mul_rscale(value, &x);
            val = adt_numeric::kernel_var::mul_var(mcx, value, &x, rscale)?;
            num.pre += num.multi;
        }

        let mut x = val.clone();
        adt_numeric::kernel_var::round_var(&mut x, num.post);
        // C: `orgnum = numeric_out(x)`, which renders NaN/Infinity specially;
        // `get_str_from_var` (used for the finite case) renders specials as "0".
        let orgnum = {
            use ::types_numeric::var::NumericSign;
            match x.sign {
                NumericSign::NaN => "NaN".to_string(),
                NumericSign::PInf => "Infinity".to_string(),
                NumericSign::NInf => "-Infinity".to_string(),
                NumericSign::Pos | NumericSign::Neg => {
                    adt_numeric::io::get_str_from_var(&x)
                }
            }
        };
        let (s, sgn) = if let Some(stripped) = orgnum.strip_prefix('-') {
            (stripped.to_string(), b'-' as i32)
        } else {
            (orgnum.clone(), b'+' as i32)
        };
        sign = sgn;
        let sb = s.into_bytes();
        let numstr_pre_len = match sb.iter().position(|&c| c == b'.') {
            Some(p) => p,
            None => sb.len(),
        };
        if (numstr_pre_len as i32) < num.pre {
            out_pre_spaces = num.pre - numstr_pre_len as i32;
            numstr = sb;
        } else if (numstr_pre_len as i32) > num.pre {
            let mut ns = fill_str(b'#', (num.pre + num.post + 1) as usize);
            if (num.pre as usize) < ns.len() {
                ns[num.pre as usize] = b'.';
            }
            numstr = ns;
        } else {
            numstr = sb;
        }
    }

    num_tochar_finish(&format, &mut num, numstr, out_pre_spaces, sign, len, collid)
}

/// C: `int4_to_char` (formatting.c:6511) core.
pub fn int4_to_char(value: i32, fmt: &[u8], collid: Oid) -> PgResult<Vec<u8>> {
    let len = fmt.len();
    if len == 0 || len >= (i32::MAX as usize - 4) / NUM_MAX_ITEM_SIZ {
        return Ok(Vec::new());
    }
    let (format, mut num) = num_cache(len, fmt)?;

    let mut out_pre_spaces = 0i32;
    let mut sign = 0i32;
    let numstr: Vec<u8>;

    if num.is_roman() {
        numstr = int_to_roman(value);
    } else if num.is_eeee() {
        let val = value as f64;
        let mut orgnum = fmt_plus_e(num.post as usize, val).into_bytes();
        if orgnum.first() == Some(&b'+') {
            orgnum[0] = b' ';
        }
        numstr = orgnum;
    } else {
        let mut orgnum: Vec<u8>;
        if num.is_multi() {
            let multi = 10f64.powi(num.multi) as i32;
            orgnum = (value.wrapping_mul(multi)).to_string().into_bytes();
            num.pre += num.multi;
        } else {
            orgnum = value.to_string().into_bytes();
        }
        if orgnum.first() == Some(&b'-') {
            sign = b'-' as i32;
            orgnum.remove(0);
        } else {
            sign = b'+' as i32;
        }
        let numstr_pre_len = orgnum.len();
        let padded = pad_post(orgnum, numstr_pre_len, &num);

        let (np, ns) = adjust_pre(padded, numstr_pre_len, &num);
        out_pre_spaces = np;
        return num_tochar_finish(&format, &mut num, ns, out_pre_spaces, sign, len, collid);
    }

    num_tochar_finish(&format, &mut num, numstr, out_pre_spaces, sign, len, collid)
}

/// C: `int8_to_char` (formatting.c:6605) core.
pub fn int8_to_char<'mcx>(
    mcx: Mcx<'mcx>,
    value: i64,
    fmt: &[u8],
    collid: Oid,
) -> PgResult<Vec<u8>> {
    let len = fmt.len();
    if len == 0 || len >= (i32::MAX as usize - 4) / NUM_MAX_ITEM_SIZ {
        return Ok(Vec::new());
    }
    let (format, mut num) = num_cache(len, fmt)?;

    let mut out_pre_spaces = 0i32;
    let mut sign = 0i32;
    let numstr: Vec<u8>;

    let mut value = value;
    if num.is_roman() {
        let intvalue = if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
            value as i32
        } else {
            i32::MAX
        };
        numstr = int_to_roman(intvalue);
    } else if num.is_eeee() {
        let v = adt_numeric::kernel_transcendental::int64_to_numericvar(mcx, value)?;
        let orgnum = numeric_out_sci_var(&v, num.post)?;
        if !orgnum.starts_with('-') {
            let mut ns = Vec::with_capacity(orgnum.len() + 1);
            ns.push(b' ');
            ns.extend_from_slice(orgnum.as_bytes());
            numstr = ns;
        } else {
            numstr = orgnum.into_bytes();
        }
    } else {
        if num.is_multi() {
            let multi = 10f64.powi(num.multi);
            // int8mul(value, dtoi8(multi))
            value = value.wrapping_mul(multi as i64);
            num.pre += num.multi;
        }
        let mut orgnum = value.to_string().into_bytes();
        if orgnum.first() == Some(&b'-') {
            sign = b'-' as i32;
            orgnum.remove(0);
        } else {
            sign = b'+' as i32;
        }
        let numstr_pre_len = orgnum.len();
        let padded = pad_post(orgnum, numstr_pre_len, &num);
        let (np, ns) = adjust_pre(padded, numstr_pre_len, &num);
        out_pre_spaces = np;
        return num_tochar_finish(&format, &mut num, ns, out_pre_spaces, sign, len, collid);
    }

    num_tochar_finish(&format, &mut num, numstr, out_pre_spaces, sign, len, collid)
}

/// C: `float4_to_char` (formatting.c:6717) core.
pub fn float4_to_char(value: f32, fmt: &[u8], collid: Oid) -> PgResult<Vec<u8>> {
    let len = fmt.len();
    if len == 0 || len >= (i32::MAX as usize - 4) / NUM_MAX_ITEM_SIZ {
        return Ok(Vec::new());
    }
    let (format, mut num) = num_cache(len, fmt)?;
    let mut out_pre_spaces = 0i32;
    let mut sign = 0i32;
    let numstr: Vec<u8>;
    let mut value = value;

    const FLT_DIG: i32 = 6;

    if num.is_roman() {
        value = value.round_ties_even();
        let intvalue = if !value.is_nan() && float4_fits_in_int32(value) {
            value as i32
        } else {
            i32::MAX
        };
        numstr = int_to_roman(intvalue);
    } else if num.is_eeee() {
        if value.is_nan() || value.is_infinite() {
            let mut ns = fill_str(b'#', (num.pre + num.post + 6) as usize);
            ns[0] = b' ';
            let dot = (num.pre + 1) as usize;
            if dot < ns.len() {
                ns[dot] = b'.';
            }
            numstr = ns;
        } else {
            let mut ns = fmt_plus_e(num.post as usize, value as f64).into_bytes();
            if ns.first() == Some(&b'+') {
                ns[0] = b' ';
            }
            numstr = ns;
        }
    } else {
        let mut val = value;
        if num.is_multi() {
            let multi = 10f32.powi(num.multi);
            val = value * multi;
            num.pre += num.multi;
        }
        let pre = fmt_f0(val.abs() as f64);
        let mut numstr_pre_len = pre.len() as i32;
        if numstr_pre_len >= FLT_DIG {
            num.post = 0;
        } else if numstr_pre_len + num.post > FLT_DIG {
            num.post = FLT_DIG - numstr_pre_len;
        }
        let orgnum = fmt_f(num.post as usize, val as f64).into_bytes();
        let (sb, sgn) = if orgnum.first() == Some(&b'-') {
            (orgnum[1..].to_vec(), b'-' as i32)
        } else {
            (orgnum, b'+' as i32)
        };
        sign = sgn;
        numstr_pre_len = match sb.iter().position(|&c| c == b'.') {
            Some(p) => p as i32,
            None => sb.len() as i32,
        };
        let (np, ns) = adjust_pre_float(sb, numstr_pre_len, &num);
        out_pre_spaces = np;
        return num_tochar_finish(&format, &mut num, ns, out_pre_spaces, sign, len, collid);
    }

    num_tochar_finish(&format, &mut num, numstr, out_pre_spaces, sign, len, collid)
}

/// C: `float8_to_char` (formatting.c:6830) core.
pub fn float8_to_char(value: f64, fmt: &[u8], collid: Oid) -> PgResult<Vec<u8>> {
    let len = fmt.len();
    if len == 0 || len >= (i32::MAX as usize - 4) / NUM_MAX_ITEM_SIZ {
        return Ok(Vec::new());
    }
    let (format, mut num) = num_cache(len, fmt)?;
    let mut out_pre_spaces = 0i32;
    let mut sign = 0i32;
    let numstr: Vec<u8>;
    let mut value = value;

    const DBL_DIG: i32 = 15;

    if num.is_roman() {
        value = value.round_ties_even();
        let intvalue = if !value.is_nan() && float8_fits_in_int32(value) {
            value as i32
        } else {
            i32::MAX
        };
        numstr = int_to_roman(intvalue);
    } else if num.is_eeee() {
        if value.is_nan() || value.is_infinite() {
            let mut ns = fill_str(b'#', (num.pre + num.post + 6) as usize);
            ns[0] = b' ';
            let dot = (num.pre + 1) as usize;
            if dot < ns.len() {
                ns[dot] = b'.';
            }
            numstr = ns;
        } else {
            let mut ns = fmt_plus_e(num.post as usize, value).into_bytes();
            if ns.first() == Some(&b'+') {
                ns[0] = b' ';
            }
            numstr = ns;
        }
    } else {
        let mut val = value;
        if num.is_multi() {
            let multi = 10f64.powi(num.multi);
            val = value * multi;
            num.pre += num.multi;
        }
        let pre = fmt_f0(val.abs());
        let mut numstr_pre_len = pre.len() as i32;
        if numstr_pre_len >= DBL_DIG {
            num.post = 0;
        } else if numstr_pre_len + num.post > DBL_DIG {
            num.post = DBL_DIG - numstr_pre_len;
        }
        let orgnum = fmt_f(num.post as usize, val).into_bytes();
        let (sb, sgn) = if orgnum.first() == Some(&b'-') {
            (orgnum[1..].to_vec(), b'-' as i32)
        } else {
            (orgnum, b'+' as i32)
        };
        sign = sgn;
        numstr_pre_len = match sb.iter().position(|&c| c == b'.') {
            Some(p) => p as i32,
            None => sb.len() as i32,
        };
        let (np, ns) = adjust_pre_float(sb, numstr_pre_len, &num);
        out_pre_spaces = np;
        return num_tochar_finish(&format, &mut num, ns, out_pre_spaces, sign, len, collid);
    }

    num_tochar_finish(&format, &mut num, numstr, out_pre_spaces, sign, len, collid)
}

// ---------------------------------------------------------------------------
// Shared int4/int8/float helpers.
// ---------------------------------------------------------------------------

/// Pad post-decimal zeros (the int4/int8 "Num.post" branch).
fn pad_post(orgnum: Vec<u8>, numstr_pre_len: usize, num: &NUMDesc) -> Vec<u8> {
    if num.post != 0 {
        let mut ns = Vec::with_capacity(numstr_pre_len + num.post as usize + 2);
        ns.extend_from_slice(&orgnum);
        ns.push(b'.');
        ns.extend(core::iter::repeat_n(b'0', num.post as usize));
        ns
    } else {
        orgnum
    }
}

/// Apply prefix padding / overflow for int4/int8. Returns (out_pre_spaces,
/// numstr).
fn adjust_pre(numstr: Vec<u8>, numstr_pre_len: usize, num: &NUMDesc) -> (i32, Vec<u8>) {
    if (numstr_pre_len as i32) < num.pre {
        (num.pre - numstr_pre_len as i32, numstr)
    } else if (numstr_pre_len as i32) > num.pre {
        let mut ns = fill_str(b'#', (num.pre + num.post + 1) as usize);
        if (num.pre as usize) < ns.len() {
            ns[num.pre as usize] = b'.';
        }
        (0, ns)
    } else {
        (0, numstr)
    }
}

/// Same as `adjust_pre` but the prefix length is measured to the decimal point
/// (the numeric/float branch).
fn adjust_pre_float(numstr: Vec<u8>, numstr_pre_len: i32, num: &NUMDesc) -> (i32, Vec<u8>) {
    if numstr_pre_len < num.pre {
        (num.pre - numstr_pre_len, numstr)
    } else if numstr_pre_len > num.pre {
        let mut ns = fill_str(b'#', (num.pre + num.post + 1) as usize);
        if (num.pre as usize) < ns.len() {
            ns[num.pre as usize] = b'.';
        }
        (0, ns)
    } else {
        (0, numstr)
    }
}

fn numeric_out_sci_var(value: &NumericVar, scale: i32) -> PgResult<String> {
    // C `numeric_to_char` calls `numeric_out_sci(value, Num.post)`, whose head
    // renders the special values; only the finite path reaches
    // `get_str_from_var_sci` (which asserts non-special). Mirror that here, since
    // the formatting caller holds a `NumericVar` rather than the on-disk image.
    use ::types_numeric::var::NumericSign;
    match value.sign {
        NumericSign::NaN => return Ok("NaN".to_string()),
        NumericSign::PInf => return Ok("Infinity".to_string()),
        NumericSign::NInf => return Ok("-Infinity".to_string()),
        NumericSign::Pos | NumericSign::Neg => {}
    }
    adt_numeric::io::get_str_from_var_sci(value, scale)
}

/// `numeric_int4_opt_error` over a `NumericVar`: round to int, returning `None`
/// on special or overflow (so the caller substitutes `PG_INT32_MAX`).
fn numericvar_to_int4_opt(value: &NumericVar) -> PgResult<Option<i32>> {
    if value.is_special() {
        return Ok(None);
    }
    // numeric_int4 rounds; emulate by rounding to scale 0 (in place) then
    // converting.
    let mut rounded = value.clone();
    adt_numeric::kernel_var::round_var(&mut rounded, 0);
    adt_numeric::convert::numericvar_to_int32(&rounded)
}

/// Select an rscale for `a * b` that matches numeric_mul's default (the sum of
/// the operands' dscales, the C default used by `numeric_mul`).
fn select_mul_rscale(a: &NumericVar, b: &NumericVar) -> i32 {
    a.dscale + b.dscale
}

#[inline]
fn float4_fits_in_int32(v: f32) -> bool {
    // C: FLOAT4_FITS_IN_INT32
    v >= -2147483648.0 && v < 2147483648.0
}
#[inline]
fn float8_fits_in_int32(v: f64) -> bool {
    v >= -2147483648.0 && v < 2147483648.0
}
