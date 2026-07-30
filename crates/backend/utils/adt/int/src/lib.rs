// std (not no_std): the out-wrapper scratch is a const-init thread_local.
extern crate alloc;

pub mod builtins;
pub mod series;

#[cfg(test)]
mod tests;

use alloc::boxed::Box;
use alloc::format;

use ::mcx::{Mcx, PgVec};
use ::numutils::{pg_itoa, pg_ltoa, pg_strtoint16_safe, pg_strtoint32_safe};
use ::types_core::{Oid, INT2OID};
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub const MAXINT2LEN: usize = 6;
pub const MAXINT4LEN: usize = 11;

const SHRT_MIN: i32 = i16::MIN as i32;
const SHRT_MAX: i32 = i16::MAX as i32;
const PG_INT16_MIN: i16 = i16::MIN;
const PG_INT32_MIN: i32 = i32::MIN;

macro_rules! overflow_fns {
    ($add:ident, $sub:ident, $mul:ident, $t:ty) => {
        #[inline(always)]
        pub(crate) fn $add(a: $t, b: $t, result: &mut $t) -> bool {
            let (v, o) = a.overflowing_add(b);
            *result = v;
            o
        }
        #[inline(always)]
        pub(crate) fn $sub(a: $t, b: $t, result: &mut $t) -> bool {
            let (v, o) = a.overflowing_sub(b);
            *result = v;
            o
        }
        #[inline(always)]
        pub(crate) fn $mul(a: $t, b: $t, result: &mut $t) -> bool {
            let (v, o) = a.overflowing_mul(b);
            *result = v;
            o
        }
    };
}

overflow_fns!(pg_add_s16_overflow, pg_sub_s16_overflow, pg_mul_s16_overflow, i16);
overflow_fns!(pg_add_s32_overflow, pg_sub_s32_overflow, pg_mul_s32_overflow, i32);

#[inline(always)]
pub(crate) fn pg_add_s64_overflow(a: i64, b: i64, result: &mut i64) -> bool {
    let (v, o) = a.overflowing_add(b);
    *result = v;
    o
}

#[track_caller]
#[cold]
#[inline(never)]
fn integer_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn smallint_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("smallint out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn division_by_zero() -> Box<PgError> {
    Box::new(PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO))
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_preceding_following() -> Box<PgError> {
    Box::new(
        PgError::error("invalid preceding or following size in window function")
            .with_sqlstate(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
    )
}

pub fn int2in(num: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i16> {
    pg_strtoint16_safe(num, escontext)
}

// Out cores encode into the caller's buffer (AGENTS rule 8: no intermediate
// cstring); the fmgr-shaped cstring wrappers live in `builtins`.
#[inline]
pub fn int2out(arg1: i16, a: &mut [u8]) -> usize {
    pg_itoa(arg1, a)
}

use ::datum::Bytea;
use ::stringinfo::StringInfo;

pub fn int2recv(buf: &mut StringInfo<'_>) -> PgResult<i16> {
    Ok(pqformat::pq_getmsgint(buf, 2)? as u16 as i16)
}

pub fn int2send<'mcx>(mcx: Mcx<'mcx>, arg1: i16) -> PgResult<Bytea<'mcx>> {
    let mut b = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint16(&mut b, arg1 as u16)?;
    Ok(pqformat::pq_endtypsend(b))
}

pub fn int4recv(buf: &mut StringInfo<'_>) -> PgResult<i32> {
    Ok(pqformat::pq_getmsgint(buf, 4)? as i32)
}

pub fn int4send<'mcx>(mcx: Mcx<'mcx>, arg1: i32) -> PgResult<Bytea<'mcx>> {
    let mut b = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint32(&mut b, arg1 as u32)?;
    Ok(pqformat::pq_endtypsend(b))
}

pub const INT2VECTOR_HDRSZ: usize = core::mem::size_of::<types_array::int2vector>();

#[inline]
const fn int2vector_size(n: usize) -> usize {
    INT2VECTOR_HDRSZ + n * core::mem::size_of::<i16>()
}

pub fn buildint2vector<'mcx>(mcx: Mcx<'mcx>, int2s: &[i16]) -> PgResult<PgVec<'mcx, u8>> {
    let n = int2s.len();
    let size = int2vector_size(n);
    let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    if v.try_reserve_exact(size).is_err() {
        return Err(Box::new(mcx.oom(size)));
    }
    let hdr = types_array::int2vector {
        vl_len_: i32::from_ne_bytes(::datum::varlena::set_varsize_4b(size)),
        ndim: 1,
        dataoffset: 0,
        elemtype: INT2OID,
        dim1: n as i32,
        // Historical int2vector convention: index lower bound 0, not 1.
        lbound1: 0,
    };
    // SAFETY: `size` bytes reserved; header + n*2 value bytes exactly fill
    // them. Byte buffer has no 4-alignment, so the header goes unaligned.
    unsafe {
        let p = v.as_mut_ptr();
        core::ptr::write_unaligned(p.cast::<types_array::int2vector>(), hdr);
        core::ptr::copy_nonoverlapping(
            int2s.as_ptr().cast::<u8>(),
            p.add(INT2VECTOR_HDRSZ),
            n * core::mem::size_of::<i16>(),
        );
        v.set_len(size);
    }
    Ok(v)
}

pub fn check_valid_int2vector(ndim: i32, dataoffset: i32, elemtype: Oid) -> PgResult<()> {
    if ndim != 1 || dataoffset != 0 || elemtype != INT2OID {
        return Err(Box::new(
            PgError::error("array is not a valid int2vector")
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
        ));
    }
    Ok(())
}

// C strtol(s, &endp, 10): optional sign then decimal digits; returns the value
// (i64-saturating, so the caller's SHRT range check mirrors ERANGE) and bytes
// consumed (0 == "endp == start").
fn strtol_base10(s: &[u8]) -> (i64, usize) {
    let mut i = 0usize;
    let mut neg = false;
    match s.first() {
        Some(b'+') => i = 1,
        Some(b'-') => {
            neg = true;
            i = 1;
        }
        _ => {}
    }
    let digit_start = i;
    let mut acc: i64 = 0;
    let mut overflow = false;
    while i < s.len() && s[i].is_ascii_digit() {
        let d = (s[i] - b'0') as i64;
        acc = match acc.checked_mul(10).and_then(|v| v.checked_add(d)) {
            Some(v) => v,
            None => {
                overflow = true;
                i64::MAX
            }
        };
        i += 1;
    }
    if i == digit_start {
        return (0, 0);
    }
    let val = if overflow {
        if neg { i64::MIN } else { i64::MAX }
    } else if neg {
        -acc
    } else {
        acc
    };
    (val, i)
}

fn soft_or_hard<T>(escontext: Option<&mut SoftErrorContext>, err: PgError) -> PgResult<Option<T>> {
    ereturn(escontext, None, err)
}

pub fn int2vectorin<'mcx>(
    mcx: Mcx<'mcx>,
    input: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let mut ints: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    let mut rest = input.as_bytes();
    loop {
        while let Some((&c, tail)) = rest.split_first() {
            if !c.is_ascii_whitespace() {
                break;
            }
            rest = tail;
        }
        if rest.is_empty() {
            break;
        }

        let shown = || alloc::string::String::from_utf8_lossy(rest).into_owned();
        let (l, consumed) = strtol_base10(rest);
        if consumed == 0 {
            return soft_or_hard(
                escontext.take(),
                PgError::error(format!(
                    "invalid input syntax for type {}: \"{}\"",
                    "smallint",
                    shown()
                ))
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
            );
        }
        if l < SHRT_MIN as i64 || l > SHRT_MAX as i64 {
            return soft_or_hard(
                escontext.take(),
                PgError::error(format!(
                    "value \"{}\" is out of range for type {}",
                    shown(),
                    "smallint"
                ))
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
            );
        }
        if let Some(&c) = rest.get(consumed) {
            if c != b' ' {
                return soft_or_hard(
                    escontext.take(),
                    PgError::error(format!(
                        "invalid input syntax for type {}: \"{}\"",
                        "smallint",
                        shown()
                    ))
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
                );
            }
        }
        if ints.try_reserve(1).is_err() {
            return Err(Box::new(mcx.oom(core::mem::size_of::<i16>())));
        }
        ints.push(l as i16);
        rest = &rest[consumed..];
    }
    Ok(Some(buildint2vector(mcx, &ints)?))
}

pub fn int2vectorout<'mcx>(
    mcx: Mcx<'mcx>,
    ndim: i32,
    dataoffset: i32,
    elemtype: Oid,
    values: &[i16],
) -> PgResult<PgVec<'mcx, u8>> {
    check_valid_int2vector(ndim, dataoffset, elemtype)?;
    let cap = values.len() * 7;
    let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    if out.try_reserve_exact(cap.max(1)).is_err() {
        return Err(Box::new(mcx.oom(cap)));
    }
    let mut len = 0usize;
    // SAFETY: each value writes at most 7 bytes (space + sign + 5 digits)
    // inside the reserved `cap`; `len` tracks the initialized prefix.
    unsafe {
        let p = out.as_mut_ptr();
        for (i, &v) in values.iter().enumerate() {
            if i != 0 {
                *p.add(len) = b' ';
                len += 1;
            }
            let mut tmp = [0u8; MAXINT2LEN];
            let n = pg_itoa(v, &mut tmp);
            core::ptr::copy_nonoverlapping(tmp.as_ptr(), p.add(len), n);
            len += n;
        }
        out.set_len(len);
    }
    Ok(out)
}

pub fn int4in(num: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i32> {
    pg_strtoint32_safe(num, escontext)
}

#[inline]
pub fn int4out(arg1: i32, a: &mut [u8]) -> usize {
    pg_ltoa(arg1, a)
}

#[inline]
pub fn i2toi4(arg1: i16) -> i32 {
    arg1 as i32
}

#[inline]
pub fn i4toi2(arg1: i32) -> PgResult<i16> {
    if arg1 < SHRT_MIN || arg1 > SHRT_MAX {
        return Err(smallint_out_of_range());
    }
    Ok(arg1 as i16)
}

#[inline]
pub fn int4_bool(arg: i32) -> bool {
    arg != 0
}

#[inline]
pub fn bool_int4(arg: bool) -> i32 {
    if arg { 1 } else { 0 }
}

macro_rules! cmp_ops {
    ($($name:ident($ta:ty, $tb:ty): $op:tt;)*) => {$(
        #[inline]
        pub fn $name(arg1: $ta, arg2: $tb) -> bool {
            (arg1 as i32) $op (arg2 as i32)
        }
    )*};
}

cmp_ops! {
    int4eq(i32, i32): ==; int4ne(i32, i32): !=;
    int4lt(i32, i32): <;  int4le(i32, i32): <=;
    int4gt(i32, i32): >;  int4ge(i32, i32): >=;
    int2eq(i16, i16): ==; int2ne(i16, i16): !=;
    int2lt(i16, i16): <;  int2le(i16, i16): <=;
    int2gt(i16, i16): >;  int2ge(i16, i16): >=;
    int24eq(i16, i32): ==; int24ne(i16, i32): !=;
    int24lt(i16, i32): <;  int24le(i16, i32): <=;
    int24gt(i16, i32): >;  int24ge(i16, i32): >=;
    int42eq(i32, i16): ==; int42ne(i32, i16): !=;
    int42lt(i32, i16): <;  int42le(i32, i16): <=;
    int42gt(i32, i16): >;  int42ge(i32, i16): >=;
}

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
    if pg_add_s32_overflow(base, offset, &mut sum) {
        // Overflowed sum is certainly past val in step's direction.
        return Ok(if sub { !less } else { less });
    }
    Ok(if less { val <= sum } else { val >= sum })
}

pub fn in_range_int4_int2(val: i32, base: i32, offset: i16, sub: bool, less: bool) -> PgResult<bool> {
    in_range_int4_int4(val, base, offset as i32, sub, less)
}

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
    if pg_add_s64_overflow(base, offset, &mut sum) {
        return Ok(if sub { !less } else { less });
    }
    Ok(if less { val <= sum } else { val >= sum })
}

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
    if pg_add_s32_overflow(base, offset, &mut sum) {
        return Ok(if sub { !less } else { less });
    }
    Ok(if less { val <= sum } else { val >= sum })
}

pub fn in_range_int2_int2(val: i16, base: i16, offset: i16, sub: bool, less: bool) -> PgResult<bool> {
    in_range_int2_int4(val, base, offset as i32, sub, less)
}

pub fn in_range_int2_int8(val: i16, base: i16, offset: i64, sub: bool, less: bool) -> PgResult<bool> {
    in_range_int4_int8(val as i32, base as i32, offset, sub, less)
}

#[inline]
pub fn int4um(arg: i32) -> PgResult<i32> {
    if arg == PG_INT32_MIN {
        return Err(integer_out_of_range());
    }
    Ok(-arg)
}

#[inline]
pub fn int4up(arg: i32) -> i32 {
    arg
}

#[inline]
pub fn int4pl(arg1: i32, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_add_s32_overflow(arg1, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int4mi(arg1: i32, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_sub_s32_overflow(arg1, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int4mul(arg1: i32, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_mul_s32_overflow(arg1, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int4div(arg1: i32, arg2: i32) -> PgResult<i32> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // INT_MIN / -1 traps; division by -1 is negation.
    if arg2 == -1 {
        if arg1 == PG_INT32_MIN {
            return Err(integer_out_of_range());
        }
        return Ok(-arg1);
    }
    Ok(arg1 / arg2)
}

#[inline]
pub fn int4inc(arg: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_add_s32_overflow(arg, 1, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int2um(arg: i16) -> PgResult<i16> {
    if arg == PG_INT16_MIN {
        return Err(smallint_out_of_range());
    }
    Ok(-arg)
}

#[inline]
pub fn int2up(arg: i16) -> i16 {
    arg
}

#[inline]
pub fn int2pl(arg1: i16, arg2: i16) -> PgResult<i16> {
    let mut result = 0;
    if pg_add_s16_overflow(arg1, arg2, &mut result) {
        return Err(smallint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int2mi(arg1: i16, arg2: i16) -> PgResult<i16> {
    let mut result = 0;
    if pg_sub_s16_overflow(arg1, arg2, &mut result) {
        return Err(smallint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int2mul(arg1: i16, arg2: i16) -> PgResult<i16> {
    let mut result = 0;
    if pg_mul_s16_overflow(arg1, arg2, &mut result) {
        return Err(smallint_out_of_range());
    }
    Ok(result)
}

#[inline]
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

#[inline]
pub fn int24pl(arg1: i16, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_add_s32_overflow(arg1 as i32, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int24mi(arg1: i16, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_sub_s32_overflow(arg1 as i32, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int24mul(arg1: i16, arg2: i32) -> PgResult<i32> {
    let mut result = 0;
    if pg_mul_s32_overflow(arg1 as i32, arg2, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int24div(arg1: i16, arg2: i32) -> PgResult<i32> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    Ok(arg1 as i32 / arg2)
}

#[inline]
pub fn int42pl(arg1: i32, arg2: i16) -> PgResult<i32> {
    let mut result = 0;
    if pg_add_s32_overflow(arg1, arg2 as i32, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int42mi(arg1: i32, arg2: i16) -> PgResult<i32> {
    let mut result = 0;
    if pg_sub_s32_overflow(arg1, arg2 as i32, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int42mul(arg1: i32, arg2: i16) -> PgResult<i32> {
    let mut result = 0;
    if pg_mul_s32_overflow(arg1, arg2 as i32, &mut result) {
        return Err(integer_out_of_range());
    }
    Ok(result)
}

#[inline]
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

#[inline]
pub fn int4mod(arg1: i32, arg2: i32) -> PgResult<i32> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // INT_MIN % -1 traps on some machines; the answer is zero.
    if arg2 == -1 {
        return Ok(0);
    }
    Ok(arg1 % arg2)
}

#[inline]
pub fn int2mod(arg1: i16, arg2: i16) -> PgResult<i16> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    if arg2 == -1 {
        return Ok(0);
    }
    Ok(arg1 % arg2)
}

#[inline]
pub fn int4abs(arg1: i32) -> PgResult<i32> {
    if arg1 == PG_INT32_MIN {
        return Err(integer_out_of_range());
    }
    Ok(if arg1 < 0 { -arg1 } else { arg1 })
}

#[inline]
pub fn int2abs(arg1: i16) -> PgResult<i16> {
    if arg1 == PG_INT16_MIN {
        return Err(smallint_out_of_range());
    }
    Ok(if arg1 < 0 { -arg1 } else { arg1 })
}

fn int4gcd_internal(mut arg1: i32, mut arg2: i32) -> PgResult<i32> {
    // Compare absolute values in negative space (INT_MIN-safe).
    let a1 = if arg1 < 0 { arg1 } else { -arg1 };
    let a2 = if arg2 < 0 { arg2 } else { -arg2 };
    if a1 > a2 {
        core::mem::swap(&mut arg1, &mut arg2);
    }

    if arg1 == PG_INT32_MIN {
        if arg2 == 0 || arg2 == PG_INT32_MIN {
            return Err(integer_out_of_range());
        }
        // gcd(INT_MIN, -1): dodge the INT_MIN % -1 trap.
        if arg2 == -1 {
            return Ok(1);
        }
    }

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

pub fn int4gcd(arg1: i32, arg2: i32) -> PgResult<i32> {
    int4gcd_internal(arg1, arg2)
}

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

#[inline]
pub fn int2larger(arg1: i16, arg2: i16) -> i16 {
    if arg1 > arg2 { arg1 } else { arg2 }
}

#[inline]
pub fn int2smaller(arg1: i16, arg2: i16) -> i16 {
    if arg1 < arg2 { arg1 } else { arg2 }
}

#[inline]
pub fn int4larger(arg1: i32, arg2: i32) -> i32 {
    if arg1 > arg2 { arg1 } else { arg2 }
}

#[inline]
pub fn int4smaller(arg1: i32, arg2: i32) -> i32 {
    if arg1 < arg2 { arg1 } else { arg2 }
}

#[inline]
pub fn int4and(arg1: i32, arg2: i32) -> i32 {
    arg1 & arg2
}

#[inline]
pub fn int4or(arg1: i32, arg2: i32) -> i32 {
    arg1 | arg2
}

#[inline]
pub fn int4xor(arg1: i32, arg2: i32) -> i32 {
    arg1 ^ arg2
}

// C `arg1 << arg2` is UB past 31; hardware (and this port) masks the count.
#[inline]
pub fn int4shl(arg1: i32, arg2: i32) -> i32 {
    arg1.wrapping_shl(arg2 as u32)
}

#[inline]
pub fn int4shr(arg1: i32, arg2: i32) -> i32 {
    arg1.wrapping_shr(arg2 as u32)
}

#[inline]
pub fn int4not(arg1: i32) -> i32 {
    !arg1
}

#[inline]
pub fn int2and(arg1: i16, arg2: i16) -> i16 {
    arg1 & arg2
}

#[inline]
pub fn int2or(arg1: i16, arg2: i16) -> i16 {
    arg1 | arg2
}

#[inline]
pub fn int2xor(arg1: i16, arg2: i16) -> i16 {
    arg1 ^ arg2
}

#[inline]
pub fn int2not(arg1: i16) -> i16 {
    !arg1
}

// C promotes to int before shifting: `(int16) (arg1 << arg2)`.
#[inline]
pub fn int2shl(arg1: i16, arg2: i32) -> i16 {
    (arg1 as i32).wrapping_shl(arg2 as u32) as i16
}

#[inline]
pub fn int2shr(arg1: i16, arg2: i32) -> i16 {
    (arg1 as i32).wrapping_shr(arg2 as u32) as i16
}
