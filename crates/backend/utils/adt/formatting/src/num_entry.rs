//! NUM SQL entry-point cores: numeric/int4/int8/float4/float8 `to_char` and
//! `to_number`. Values in as Rust vocabulary (`Num`/`i32`/`i64`/`f32`/`f64`),
//! formatted text out as a single-image `Varlena`; `to_number` yields a packed
//! `NumericImage`.

use ::datum::Varlena;
use ::mcx::{Mcx, PgVec};
use ::numeric::{
    int64_to_numeric, make_result, make_result_into, mul_var, numeric_in, numeric_int4,
    numeric_out_into, numeric_out_sci, numeric_overflow_error, power_var, Num, NumericImage,
    NumericVar, NUMERIC_NAN, NUMERIC_NINF, NUMERIC_PINF,
};
use ::types_error::PgResult;

use crate::num::{
    fill_str, fmt_f, fmt_f0, fmt_plus_e, int_to_roman, num_processor_from_char,
    num_processor_to_char,
};
use crate::tables::*;

const VARHDRSZ: usize = ::datum::varlena::VARHDRSZ;

fn text_result<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Varlena<'mcx>> {
    let cap = VARHDRSZ + payload.len();
    let mut image: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, cap)?;
    ::mcx::vec_append_bytes(&mut image, &[0u8; VARHDRSZ])?;
    ::mcx::vec_append_bytes(&mut image, payload)?;
    Ok(Varlena::from_image(image))
}

// C: ((precision << 16) | scale) + VARHDRSZ.
fn make_numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ as i32
}

// Retained render scratch (C's per-call pallocs are bump-freed wholesale);
// borrowed only inside one to_char call, never across a re-entry point.
struct ToCharScratch {
    img: NumericImage,
    digits: Vec<u8>,
}

std::thread_local! {
    static TOCHAR_SCRATCH: std::cell::RefCell<Option<ToCharScratch>> =
        const { std::cell::RefCell::new(None) };
}

type Fmt = std::rc::Rc<[FormatNode]>;

fn num_cache(len: usize, fmt: &[u8]) -> PgResult<(Fmt, NUMDesc)> {
    if len > NUM_CACHE_SIZE {
        let mut num = NUMDesc::default();
        num.zeroize();
        let format: Fmt =
            crate::parse::parse_format(fmt, NUM_KEYWORDS, &[], &NUM_INDEX, NUM_FLAG, Some(&mut num))?
                .into();
        Ok((format, num))
    } else {
        crate::cache::num_cache_fetch(fmt)
    }
}

// C's NUM_TOCHAR_prepare/finish: one zeroed image sized (len *
// NUM_MAX_ITEM_SIZ) + 1 + VARHDRSZ, NUM_processor writes the payload in place.
fn num_tochar_finish<'mcx>(
    mcx: Mcx<'mcx>,
    format: &[FormatNode],
    num: &mut NUMDesc,
    numstr: &[u8],
    out_pre_spaces: i32,
    sign: i32,
    fmt_len: usize,
) -> PgResult<Varlena<'mcx>> {
    let size = VARHDRSZ + fmt_len * NUM_MAX_ITEM_SIZ + 1;
    let mut image: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, size)?;
    image.resize(size, 0);
    let n = num_processor_to_char(
        format,
        num,
        &mut image[VARHDRSZ..],
        numstr,
        out_pre_spaces,
        sign,
    )?;
    image.truncate(VARHDRSZ + n);
    Ok(Varlena::from_image(image))
}

fn too_big(len: usize) -> bool {
    len == 0 || len >= (i32::MAX as usize - 4) / NUM_MAX_ITEM_SIZ
}

#[cold]
#[inline(never)]
fn bigint_out_of_range() -> Box<::types_error::PgError> {
    Box::new(
        ::types_error::PgError::error("bigint out of range")
            .with_sqlstate(::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

fn numeric_out_sci_str(value: Num<'_>, scale: i32) -> Vec<u8> {
    let mut buf = Vec::new();
    numeric_out_sci(value, scale, &mut buf);
    buf
}

fn special_orgnum(v: Num<'_>) -> Option<&'static [u8]> {
    if v.is_nan() {
        Some(b"NaN")
    } else if v.is_pinf() {
        Some(b"Infinity")
    } else if v.is_ninf() {
        Some(b"-Infinity")
    } else {
        None
    }
}

fn var_special_orgnum(sign: u16) -> Option<&'static [u8]> {
    match sign {
        NUMERIC_NAN => Some(b"NaN"),
        NUMERIC_PINF => Some(b"Infinity"),
        NUMERIC_NINF => Some(b"-Infinity"),
        _ => None,
    }
}

// numeric_int4_opt_error over a Num: round to int, special/overflow -> INT32_MAX.
fn numericvar_to_int4_opt(value: Num<'_>) -> i32 {
    if value.is_special() {
        return i32::MAX;
    }
    let mut x = NumericVar::from_view(value.view());
    x.round(0);
    match make_result(x.view()) {
        Ok(img) => numeric_int4(img.num()).unwrap_or(i32::MAX),
        Err(_) => i32::MAX,
    }
}

// C: numeric_out(numeric_round(val, post)) — make_result normalizes a
// rounded-to-zero negative to canonical "0" (get_str_from_var alone keeps "-0").
fn render_var_into(x: &NumericVar, sc: &mut ToCharScratch) -> PgResult<bool> {
    if let Some(s) = var_special_orgnum(x.sign) {
        sc.digits.extend_from_slice(s);
        sc.digits.push(0);
        return Ok(true);
    }
    if !make_result_into(x.view(), &mut sc.img) {
        return Err(numeric_overflow_error().into());
    }
    numeric_out_into(sc.img.num(), &mut sc.digits);
    sc.digits.push(0);
    Ok(false)
}

// Input and both outputs are NUL-terminated (C's sign-strip by pointer bump).
fn split_sign(digits: &[u8]) -> (&[u8], i32) {
    if digits.first() == Some(&b'-') {
        (&digits[1..], b'-' as i32)
    } else {
        (digits, b'+' as i32)
    }
}

pub fn numeric_to_char<'mcx>(
    mcx: Mcx<'mcx>,
    value: Num<'_>,
    fmt: &[u8],
) -> PgResult<Varlena<'mcx>> {
    let len = fmt.len();
    if too_big(len) {
        return text_result(mcx, b"");
    }
    let (format, mut num) = num_cache(len, fmt)?;

    if num.is_roman() {
        let mut numstr = int_to_roman(numericvar_to_int4_opt(value));
        numstr.push(0);
        return num_tochar_finish(mcx, &format, &mut num, &numstr, 0, 0, len);
    }
    if num.is_eeee() {
        let orgnum = numeric_out_sci_str(value, num.post);
        let mut numstr: Vec<u8>;
        if orgnum == b"NaN" || orgnum == b"Infinity" || orgnum == b"-Infinity" {
            let mut ns = fill_str(b'#', (num.pre + num.post + 6) as usize);
            ns[0] = b' ';
            let dot = (num.pre + 1) as usize;
            if dot < ns.len() {
                ns[dot] = b'.';
            }
            numstr = ns;
        } else if orgnum.first() != Some(&b'-') {
            let mut ns = Vec::with_capacity(orgnum.len() + 2);
            ns.push(b' ');
            ns.extend_from_slice(&orgnum);
            numstr = ns;
        } else {
            numstr = orgnum;
        }
        numstr.push(0);
        return num_tochar_finish(mcx, &format, &mut num, &numstr, 0, 0, len);
    }

    if num.is_multi() {
        num.pre += num.multi;
    }
    TOCHAR_SCRATCH.with(|cell| {
        let mut slot = cell.borrow_mut();
        let sc = slot.get_or_insert_with(|| ToCharScratch {
            img: NumericImage::empty(),
            digits: Vec::new(),
        });
        sc.digits.clear();

        let special = if let Some(s) = special_orgnum(value) {
            sc.digits.extend_from_slice(s);
            sc.digits.push(0);
            true
        } else if num.is_multi() {
            let ten = int64_to_numeric(10);
            let exp = int64_to_numeric(num.multi as i64);
            let mut xpow = NumericVar::new();
            power_var(ten.num().view(), exp.num().view(), &mut xpow)?;
            let rscale = value.view().dscale + xpow.dscale;
            let mut prod = NumericVar::new();
            mul_var(value.view(), xpow.view(), &mut prod, rscale);
            prod.round(num.post);
            render_var_into(&prod, sc)?
        } else {
            let mut x = NumericVar::from_view(value.view());
            x.round(num.post);
            render_var_into(&x, sc)?
        };

        let (stripped, sign) = split_sign(&sc.digits);
        // The dot sits num.post + 1 from the end (round pinned dscale, and
        // get_str_from_var emits exactly dscale decimals); specials carry no
        // dot — C's strchr/strlen without the scan.
        let strlen = stripped.len() as i32 - 1;
        let numstr_pre_len = if !special && num.post > 0 {
            strlen - num.post - 1
        } else {
            strlen
        };
        debug_assert_eq!(numstr_pre_len, pre_len(stripped));
        let hf;
        let (numstr, out_pre_spaces): (&[u8], i32) = if numstr_pre_len < num.pre {
            (stripped, num.pre - numstr_pre_len)
        } else if numstr_pre_len > num.pre {
            hf = {
                let mut v = hash_fill(&num);
                v.push(0);
                v
            };
            (&hf, 0)
        } else {
            (stripped, 0)
        };

        num_tochar_finish(mcx, &format, &mut num, numstr, out_pre_spaces, sign, len)
    })
}

fn strip_sign(mut orgnum: Vec<u8>) -> (Vec<u8>, i32) {
    if orgnum.first() == Some(&b'-') {
        orgnum.remove(0);
        (orgnum, b'-' as i32)
    } else {
        (orgnum, b'+' as i32)
    }
}

// C: strchr('.') else strlen — sb may carry a trailing NUL.
fn pre_len(sb: &[u8]) -> i32 {
    match sb.iter().position(|&c| c == b'.' || c == 0) {
        Some(p) => p as i32,
        None => sb.len() as i32,
    }
}

fn hash_fill(num: &NUMDesc) -> Vec<u8> {
    let mut ns = fill_str(b'#', (num.pre + num.post + 1) as usize);
    if (num.pre as usize) < ns.len() {
        ns[num.pre as usize] = b'.';
    }
    ns
}

pub fn int4_to_char<'mcx>(mcx: Mcx<'mcx>, value: i32, fmt: &[u8]) -> PgResult<Varlena<'mcx>> {
    let len = fmt.len();
    if too_big(len) {
        return text_result(mcx, b"");
    }
    let (format, mut num) = num_cache(len, fmt)?;

    let mut out_pre_spaces = 0i32;
    let mut sign = 0i32;
    let mut numstr: Vec<u8>;

    if num.is_roman() {
        numstr = int_to_roman(value);
    } else if num.is_eeee() {
        let mut orgnum = fmt_plus_e(num.post as usize, value as f64).into_bytes();
        if orgnum.first() == Some(&b'+') {
            orgnum[0] = b' ';
        }
        numstr = orgnum;
    } else {
        let mut orgnum: Vec<u8>;
        if num.is_multi() {
            let multi = 10f64.powi(num.multi) as i32;
            orgnum = value.wrapping_mul(multi).to_string().into_bytes();
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
        let pre = orgnum.len();
        let padded = pad_post(orgnum, pre, &num);
        let (np, overflowed) = adjust_pre(pre as i32, &num);
        out_pre_spaces = np;
        let mut ns = if overflowed { hash_fill(&num) } else { padded };
        ns.push(0);
        return num_tochar_finish(mcx, &format, &mut num, &ns, out_pre_spaces, sign, len);
    }

    numstr.push(0);
    num_tochar_finish(mcx, &format, &mut num, &numstr, out_pre_spaces, sign, len)
}

pub fn int8_to_char<'mcx>(mcx: Mcx<'mcx>, value: i64, fmt: &[u8]) -> PgResult<Varlena<'mcx>> {
    let len = fmt.len();
    if too_big(len) {
        return text_result(mcx, b"");
    }
    let (format, mut num) = num_cache(len, fmt)?;

    let mut out_pre_spaces = 0i32;
    let mut sign = 0i32;
    let mut numstr: Vec<u8>;

    let mut value = value;
    if num.is_roman() {
        let intvalue = if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
            value as i32
        } else {
            i32::MAX
        };
        numstr = int_to_roman(intvalue);
    } else if num.is_eeee() {
        let v = int64_to_numeric(value);
        let orgnum = numeric_out_sci_str(v.num(), num.post);
        if orgnum.first() != Some(&b'-') {
            let mut ns = Vec::with_capacity(orgnum.len() + 1);
            ns.push(b' ');
            ns.extend_from_slice(&orgnum);
            numstr = ns;
        } else {
            numstr = orgnum;
        }
    } else {
        if num.is_multi() {
            // C: int8mul(value, dtoi8(pow(10, multi))) — both raise 22003.
            let multi = 10f64.powi(num.multi).round_ties_even();
            let m = if multi >= -9.223372036854776e18 && multi < 9.223372036854776e18 {
                multi as i64
            } else {
                return Err(bigint_out_of_range());
            };
            value = value.checked_mul(m).ok_or_else(bigint_out_of_range)?;
            num.pre += num.multi;
        }
        let mut orgnum = value.to_string().into_bytes();
        if orgnum.first() == Some(&b'-') {
            sign = b'-' as i32;
            orgnum.remove(0);
        } else {
            sign = b'+' as i32;
        }
        let pre = orgnum.len();
        let padded = pad_post(orgnum, pre, &num);
        let (np, overflowed) = adjust_pre(pre as i32, &num);
        out_pre_spaces = np;
        let mut ns = if overflowed { hash_fill(&num) } else { padded };
        ns.push(0);
        return num_tochar_finish(mcx, &format, &mut num, &ns, out_pre_spaces, sign, len);
    }

    numstr.push(0);
    num_tochar_finish(mcx, &format, &mut num, &numstr, out_pre_spaces, sign, len)
}

pub fn float4_to_char<'mcx>(mcx: Mcx<'mcx>, value: f32, fmt: &[u8]) -> PgResult<Varlena<'mcx>> {
    let len = fmt.len();
    if too_big(len) {
        return text_result(mcx, b"");
    }
    let (format, mut num) = num_cache(len, fmt)?;
    let mut out_pre_spaces = 0i32;
    let mut sign = 0i32;
    let mut numstr: Vec<u8>;
    let mut value = value;

    const FLT_DIG: i32 = 6;

    if num.is_roman() {
        value = value.round_ties_even();
        let intvalue = if !value.is_nan() && value >= -2147483648.0 && value < 2147483648.0 {
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
            val = value * 10f32.powi(num.multi);
            num.pre += num.multi;
        }
        let pre = fmt_f0(val.abs() as f64);
        let numstr_pre_len = pre.len() as i32;
        if numstr_pre_len >= FLT_DIG {
            num.post = 0;
        } else if numstr_pre_len + num.post > FLT_DIG {
            num.post = FLT_DIG - numstr_pre_len;
        }
        let orgnum = fmt_f(num.post as usize, val as f64).into_bytes();
        let (sb, sgn) = strip_sign(orgnum);
        sign = sgn;
        let (np, overflowed) = adjust_pre(pre_len(&sb), &num);
        out_pre_spaces = np;
        let mut ns = if overflowed { hash_fill(&num) } else { sb };
        ns.push(0);
        return num_tochar_finish(mcx, &format, &mut num, &ns, out_pre_spaces, sign, len);
    }

    numstr.push(0);
    num_tochar_finish(mcx, &format, &mut num, &numstr, out_pre_spaces, sign, len)
}

pub fn float8_to_char<'mcx>(mcx: Mcx<'mcx>, value: f64, fmt: &[u8]) -> PgResult<Varlena<'mcx>> {
    let len = fmt.len();
    if too_big(len) {
        return text_result(mcx, b"");
    }
    let (format, mut num) = num_cache(len, fmt)?;
    let mut out_pre_spaces = 0i32;
    let mut sign = 0i32;
    let mut numstr: Vec<u8>;
    let mut value = value;

    const DBL_DIG: i32 = 15;

    if num.is_roman() {
        value = value.round_ties_even();
        let intvalue = if !value.is_nan() && value >= -2147483648.0 && value < 2147483648.0 {
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
            val = value * 10f64.powi(num.multi);
            num.pre += num.multi;
        }
        let pre = fmt_f0(val.abs());
        let numstr_pre_len = pre.len() as i32;
        if numstr_pre_len >= DBL_DIG {
            num.post = 0;
        } else if numstr_pre_len + num.post > DBL_DIG {
            num.post = DBL_DIG - numstr_pre_len;
        }
        let orgnum = fmt_f(num.post as usize, val).into_bytes();
        let (sb, sgn) = strip_sign(orgnum);
        sign = sgn;
        let (np, overflowed) = adjust_pre(pre_len(&sb), &num);
        out_pre_spaces = np;
        let mut ns = if overflowed { hash_fill(&num) } else { sb };
        ns.push(0);
        return num_tochar_finish(mcx, &format, &mut num, &ns, out_pre_spaces, sign, len);
    }

    numstr.push(0);
    num_tochar_finish(mcx, &format, &mut num, &numstr, out_pre_spaces, sign, len)
}

fn pad_post(orgnum: Vec<u8>, pre: usize, num: &NUMDesc) -> Vec<u8> {
    if num.post != 0 {
        let mut ns = Vec::with_capacity(pre + num.post as usize + 2);
        ns.extend_from_slice(&orgnum);
        ns.push(b'.');
        ns.extend(core::iter::repeat_n(b'0', num.post as usize));
        ns
    } else {
        orgnum
    }
}

fn adjust_pre(pre: i32, num: &NUMDesc) -> (i32, bool) {
    if pre < num.pre {
        (num.pre - pre, false)
    } else {
        (0, pre > num.pre)
    }
}

/// C: `numeric_to_number` (formatting.c). C returns SQL NULL for an empty or
/// oversized fmt; the required signature has no Option, so that degenerate case
/// yields numeric 0 (the fmgr wrapper owns the NULL decision).
pub fn numeric_to_number<'mcx>(
    _mcx: Mcx<'mcx>,
    value: &[u8],
    fmt: &[u8],
) -> PgResult<NumericImage> {
    let len = fmt.len();
    if len == 0 || len >= (i32::MAX as usize) / NUM_MAX_ITEM_SIZ {
        return Ok(int64_to_numeric(0));
    }
    let (format, mut num) = num_cache(len, fmt)?;

    let mut numstr = vec![0u8; len * NUM_MAX_ITEM_SIZ + 1];
    let n = num_processor_from_char(&format, &mut num, value, &mut numstr)?;

    let scale = num.post;
    let precision = num.pre + num.multi + scale;

    let s = String::from_utf8_lossy(&numstr[..n]).into_owned();
    let img = numeric_in(&s, make_numeric_typmod(precision, scale), None)?
        .expect("numeric_in without soft-error context yields Some");

    if num.is_multi() {
        let ten = int64_to_numeric(10);
        let exp = int64_to_numeric(-(num.multi as i64));
        let mut xpow = NumericVar::new();
        power_var(ten.num().view(), exp.num().view(), &mut xpow)?;
        let base = img.num();
        let rscale = base.view().dscale + xpow.dscale;
        let mut prod = NumericVar::new();
        mul_var(base.view(), xpow.view(), &mut prod, rscale);
        Ok(make_result(prod.view())?)
    } else {
        Ok(img)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> ::mcx::MemoryContext {
        ::mcx::MemoryContext::new("test")
    }

    fn as_text(v: &Varlena) -> String {
        String::from_utf8_lossy(v.data()).into_owned()
    }

    #[test]
    fn int4_basic() {
        let c = ctx();
        // Positive values reserve a leading sign space (no FM/MI/PL/SG/S).
        assert_eq!(as_text(&int4_to_char(c.mcx(), 1234, b"0000").unwrap()), " 1234");
        assert_eq!(as_text(&int4_to_char(c.mcx(), 485, b"999").unwrap()), " 485");
        // RN is right-justified in a 15-wide field, no sign space.
        assert_eq!(as_text(&int4_to_char(c.mcx(), 485, b"RN").unwrap()), "        CDLXXXV");
    }

    #[test]
    fn numeric_grouping_and_sign() {
        let c = ctx();
        let v = ::numeric::numeric_in("-1234.56", -1, None).unwrap().unwrap();
        assert_eq!(
            as_text(&numeric_to_char(c.mcx(), v.num(), b"9G999D99").unwrap()),
            "-1,234.56"
        );
    }

    #[test]
    fn numeric_fm() {
        let c = ctx();
        let v = ::numeric::numeric_in("0.1", -1, None).unwrap().unwrap();
        assert_eq!(as_text(&numeric_to_char(c.mcx(), v.num(), b"FM9.99").unwrap()), ".1");
    }

    #[test]
    fn to_number_grouping() {
        let c = ctx();
        let img = numeric_to_number(c.mcx(), b"12,345.6", b"99G999D9").unwrap();
        let mut out = Vec::new();
        ::numeric::numeric_out_into(img.num(), &mut out);
        assert_eq!(String::from_utf8_lossy(&out), "12345.6");
    }
}
