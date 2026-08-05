use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

const DIGIT_TABLE: &[u8; 200] = b"\
0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

pub const MAXINT8LEN: usize = 20;

// C numutils.c's __func__ for the width. Client-keyed: pg8000's
// error-field test pins F="numutils.c" and R in the pg_strtoint*_safe
// family on integer-input errors (the errorMissingColumn precedent).
fn strtoint_funcname(typname: &str) -> &'static str {
    match typname {
        "smallint" => "pg_strtoint16_safe",
        "bigint" => "pg_strtoint64_safe",
        _ => "pg_strtoint32_safe",
    }
}

#[cold]
#[inline(never)]
fn invalid_syntax_err(input: &str, typname: &'static str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {typname}: \"{input}\""
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
    .with_location("numutils.c", 0, strtoint_funcname(typname))
}

#[cold]
#[inline(never)]
fn out_of_range_err(input: &str, typname: &'static str) -> PgError {
    PgError::error(format!(
        "value \"{input}\" is out of range for type {typname}"
    ))
    .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
    .with_location("numutils.c", 0, strtoint_funcname(typname))
}

fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

fn hexval(byte: u8) -> Option<u64> {
    match byte {
        b'0'..=b'9' => Some((byte - b'0') as u64),
        b'a'..=b'f' => Some((byte - b'a' + 10) as u64),
        b'A'..=b'F' => Some((byte - b'A' + 10) as u64),
        _ => None,
    }
}

enum NumErr {
    OutOfRange,
    InvalidSyntax,
}

// C slow path: spaces, +/-, 0x/0o/0b prefixes, '_' separators; the final
// range checks stay in the caller. u64 holds every width's magnitude.
fn strtoint_slow_inner(b: &[u8], neg_abs: u64) -> Result<(u64, bool), NumErr> {
    let len = b.len();
    let mut i = 0usize;
    while i < len && is_space(b[i]) {
        i += 1;
    }
    let mut neg = false;
    if i < len && b[i] == b'-' {
        neg = true;
        i += 1;
    } else if i < len && b[i] == b'+' {
        i += 1;
    }

    let mut tmp = 0u64;
    let prefix = (b.get(i).copied(), b.get(i + 1).copied());
    match prefix {
        (Some(b'0'), Some(b'x' | b'X')) => {
            i += 2;
            let firstdigit = i;
            loop {
                match b.get(i).copied() {
                    Some(c) if hexval(c).is_some() => {
                        if tmp > neg_abs / 16 {
                            return Err(NumErr::OutOfRange);
                        }
                        tmp = tmp * 16 + hexval(c).unwrap();
                        i += 1;
                    }
                    Some(b'_') => {
                        i += 1;
                        match b.get(i).copied() {
                            Some(c) if hexval(c).is_some() => {}
                            _ => return Err(NumErr::InvalidSyntax),
                        }
                    }
                    _ => break,
                }
            }
            if i == firstdigit {
                return Err(NumErr::InvalidSyntax);
            }
        }
        (Some(b'0'), Some(b'o' | b'O')) => {
            i += 2;
            let firstdigit = i;
            loop {
                match b.get(i).copied() {
                    Some(c @ b'0'..=b'7') => {
                        if tmp > neg_abs / 8 {
                            return Err(NumErr::OutOfRange);
                        }
                        tmp = tmp * 8 + (c - b'0') as u64;
                        i += 1;
                    }
                    Some(b'_') => {
                        i += 1;
                        match b.get(i).copied() {
                            Some(b'0'..=b'7') => {}
                            _ => return Err(NumErr::InvalidSyntax),
                        }
                    }
                    _ => break,
                }
            }
            if i == firstdigit {
                return Err(NumErr::InvalidSyntax);
            }
        }
        (Some(b'0'), Some(b'b' | b'B')) => {
            i += 2;
            let firstdigit = i;
            loop {
                match b.get(i).copied() {
                    Some(c @ b'0'..=b'1') => {
                        if tmp > neg_abs / 2 {
                            return Err(NumErr::OutOfRange);
                        }
                        tmp = tmp * 2 + (c - b'0') as u64;
                        i += 1;
                    }
                    Some(b'_') => {
                        i += 1;
                        match b.get(i).copied() {
                            Some(b'0'..=b'1') => {}
                            _ => return Err(NumErr::InvalidSyntax),
                        }
                    }
                    _ => break,
                }
            }
            if i == firstdigit {
                return Err(NumErr::InvalidSyntax);
            }
        }
        _ => {
            let firstdigit = i;
            loop {
                match b.get(i).copied() {
                    Some(c @ b'0'..=b'9') => {
                        if tmp > neg_abs / 10 {
                            return Err(NumErr::OutOfRange);
                        }
                        tmp = tmp * 10 + (c - b'0') as u64;
                        i += 1;
                    }
                    // '_' may not lead the digits in the decimal branch only.
                    Some(b'_') => {
                        if i == firstdigit {
                            return Err(NumErr::InvalidSyntax);
                        }
                        i += 1;
                        match b.get(i).copied() {
                            Some(b'0'..=b'9') => {}
                            _ => return Err(NumErr::InvalidSyntax),
                        }
                    }
                    _ => break,
                }
            }
            if i == firstdigit {
                return Err(NumErr::InvalidSyntax);
            }
        }
    }

    while i < len && is_space(b[i]) {
        i += 1;
    }
    if i != len {
        return Err(NumErr::InvalidSyntax);
    }
    Ok((tmp, neg))
}

// Outlined so the fast path never holds a PgError by value (its sret
// temporary would otherwise widen the hot function's frame).
#[cold]
#[inline(never)]
fn strtoint_oor(
    s: &str,
    typname: &'static str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<i64> {
    ereturn(escontext, 0, out_of_range_err(s, typname))
}

#[cold]
#[inline(never)]
fn strtoint_slow(
    s: &str,
    neg_abs: u64,
    max: u64,
    typname: &'static str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<i64> {
    let result = strtoint_slow_inner(s.as_bytes(), neg_abs).and_then(|(tmp, neg)| {
        if neg {
            if tmp > neg_abs {
                return Err(NumErr::OutOfRange);
            }
            Ok(0i64.wrapping_sub(tmp as i64))
        } else {
            if tmp > max {
                return Err(NumErr::OutOfRange);
            }
            Ok(tmp as i64)
        }
    });
    match result {
        Ok(v) => Ok(v),
        Err(NumErr::OutOfRange) => ereturn(escontext, 0, out_of_range_err(s, typname)),
        Err(NumErr::InvalidSyntax) => ereturn(escontext, 0, invalid_syntax_err(s, typname)),
    }
}

// Lemire's 8-digit SWAR conversion; `v` holds byte values 0..=9 (ASCII
// already stripped), first character in the low byte (little-endian load).
#[inline(always)]
fn swar_parse8(mut v: u64) -> u32 {
    const MASK: u64 = 0x000000FF000000FF;
    const MUL1: u64 = 0x000F424000000064;
    const MUL2: u64 = 0x0000271000000001;
    v = v.wrapping_mul(10).wrapping_add(v >> 8);
    ((((v & MASK).wrapping_mul(MUL1)).wrapping_add(((v >> 16) & MASK).wrapping_mul(MUL2))) >> 32)
        as u32
}

macro_rules! strtoint {
    ($plain:ident, $safe:ident, $ity:ty, $typname:literal) => {
        pub fn $plain(s: &str) -> PgResult<$ity> {
            $safe(s, None)
        }

        pub fn $safe(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<$ity> {
            const NEG_ABS: u64 = <$ity>::MIN.unsigned_abs() as u64;
            const MAX: u64 = <$ity>::MAX as u64;
            const GUARD: u64 = NEG_ABS / 10;
            // C's per-digit guard trips iff tmp >= (GUARD+1)*10 at the break,
            // so it is deferred out of the hot loop; equivalence argument in
            // docs/optimizations/numutils-parity.md.
            const BREAK_OOR: u64 = (GUARD + 1) * 10;

            let b = s.as_bytes();
            let len = b.len();
            let mut i = 0usize;
            let mut neg = false;
            if len != 0 && b[0] == b'-' {
                neg = true;
                i = 1;
            }
            let digits_start = i;
            let mut tmp: u64 = 0;
            // Digit run consumed as SWAR blocks (4 then 8 wide) plus a scalar
            // tail; a non-digit byte fails a block's validity mask and falls
            // through, where the scalar loop finds it again. >19 total digits
            // could wrap the u64 accumulator; punt to the slow path, which
            // implements the identical grammar with C's per-digit guard.
            // <=19 digits fit u64 exactly, so everything runs guard-free.
            if len - i >= 8 {
                if len - i > 19 {
                    return strtoint_slow(s, NEG_ABS, MAX, $typname, escontext)
                        .map(|v| v as $ity);
                }
                loop {
                    // SAFETY: i + 8 <= len.
                    let chunk =
                        unsafe { core::ptr::read_unaligned(b.as_ptr().add(i) as *const u64) };
                    let t = chunk ^ 0x3030303030303030;
                    if (t.wrapping_add(0x7676767676767676) | t) & 0x8080808080808080 != 0 {
                        break;
                    }
                    tmp = tmp * 100_000_000 + swar_parse8(t) as u64;
                    i += 8;
                    if len - i < 8 {
                        break;
                    }
                }
            }
            if len - i >= 4 {
                // SAFETY: i + 4 <= len.
                let chunk =
                    unsafe { core::ptr::read_unaligned(b.as_ptr().add(i) as *const u32) };
                let t = chunk ^ 0x30303030;
                if (t.wrapping_add(0x76767676) | t) & 0x80808080 == 0 {
                    let v = (t.wrapping_mul(10).wrapping_add(t >> 8)) & 0x00FF00FF;
                    tmp = tmp * 10_000 + ((v & 0xFF) * 100 + (v >> 16)) as u64;
                    i += 4;
                }
            }
            for &c in &b[i..] {
                let d = c.wrapping_sub(b'0');
                if d >= 10 {
                    break;
                }
                tmp = tmp * 10 + d as u64;
                i += 1;
            }
            if i == digits_start {
                return strtoint_slow(s, NEG_ABS, MAX, $typname, escontext).map(|v| v as $ity);
            }
            if i != len {
                if tmp >= BREAK_OOR {
                    return strtoint_oor(s, $typname, escontext).map(|v| v as $ity);
                }
                return strtoint_slow(s, NEG_ABS, MAX, $typname, escontext).map(|v| v as $ity);
            }
            if neg {
                if tmp > NEG_ABS {
                    return strtoint_oor(s, $typname, escontext).map(|v| v as $ity);
                }
                Ok(0u64.wrapping_sub(tmp) as $ity)
            } else {
                if tmp > MAX {
                    return strtoint_oor(s, $typname, escontext).map(|v| v as $ity);
                }
                Ok(tmp as $ity)
            }
        }
    };
}

strtoint!(pg_strtoint16, pg_strtoint16_safe, i16, "smallint");
strtoint!(pg_strtoint32, pg_strtoint32_safe, i32, "integer");
strtoint!(pg_strtoint64, pg_strtoint64_safe, i64, "bigint");

// C strtoul/strtou64 base-0 model + the uint*in_subr checks; cold (oid/xid
// input paths). When `endloc` the unconsumed tail is returned, else only
// trailing whitespace may follow.
fn uintin_subr<'a>(
    s: &'a str,
    is_u32: bool,
    endloc: bool,
) -> Result<(u64, &'a str), NumErr> {
    let b = s.as_bytes();
    let len = b.len();
    let mut i = 0usize;
    while i < len && is_space(b[i]) {
        i += 1;
    }
    let mut neg = false;
    if i < len && b[i] == b'-' {
        neg = true;
        i += 1;
    } else if i < len && b[i] == b'+' {
        i += 1;
    }

    // Base 0: 0x/0X is hex only when a hex digit follows (a bare "0x"
    // backtracks: the 0 parses, endptr lands on the 'x'); a bare leading 0 is
    // octal and is itself the first digit; otherwise decimal.
    let (base, digit_start) = match (b.get(i).copied(), b.get(i + 1).copied()) {
        (Some(b'0'), Some(b'x' | b'X'))
            if b.get(i + 2).copied().is_some_and(|c| hexval(c).is_some()) =>
        {
            i += 2;
            (16u64, i)
        }
        (Some(b'0'), _) => (8u64, i),
        _ => (10u64, i),
    };

    let mut cvt = 0u64;
    while i < len {
        let d = match b[i] {
            c if base == 16 => match hexval(c) {
                Some(d) => d,
                None => break,
            },
            c @ b'0'..=b'9' if ((c - b'0') as u64) < base => (c - b'0') as u64,
            _ => break,
        };
        if cvt > (u64::MAX - d) / base {
            return Err(NumErr::OutOfRange);
        }
        cvt = cvt * base + d;
        i += 1;
    }
    if i == digit_start {
        return Err(NumErr::InvalidSyntax);
    }
    let endptr = i;

    if !endloc {
        while i < len && is_space(b[i]) {
            i += 1;
        }
        if i != len {
            return Err(NumErr::InvalidSyntax);
        }
    }

    let value = if neg { 0u64.wrapping_sub(cvt) } else { cvt };
    let value = if is_u32 {
        // C accepts a 64-bit cvt that round-trips through uint32 after either
        // zero- or sign-extension back to long (backwards-compat minus sign).
        let result = value as u32;
        if value != result as u64 && value != result as i32 as i64 as u64 {
            return Err(NumErr::OutOfRange);
        }
        result as u64
    } else {
        value
    };
    Ok((value, &s[endptr..]))
}

fn uintin_report<'a>(
    r: Result<(u64, &'a str), NumErr>,
    s: &'a str,
    typname: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<(u64, &'a str)> {
    match r {
        Ok(v) => Ok(v),
        Err(NumErr::OutOfRange) => ereturn(
            escontext,
            (0, ""),
            PgError::error(format!(
                "value \"{s}\" is out of range for type {typname}"
            ))
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
        ),
        Err(NumErr::InvalidSyntax) => ereturn(
            escontext,
            (0, ""),
            PgError::error(format!(
                "invalid input syntax for type {typname}: \"{s}\""
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ),
    }
}

pub fn uint32in_subr<'a>(
    s: &'a str,
    endloc: bool,
    typname: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<(u32, &'a str)> {
    let (v, rest) = uintin_report(uintin_subr(s, true, endloc), s, typname, escontext)?;
    Ok((v as u32, rest))
}

pub fn uint64in_subr<'a>(
    s: &'a str,
    endloc: bool,
    typname: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<(u64, &'a str)> {
    uintin_report(uintin_subr(s, false, endloc), s, typname, escontext)
}

fn decimal_length32(v: u32) -> usize {
    const POWERS_OF_TEN: [u32; 10] = [
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
    ];
    let t = ((32 - v.leading_zeros() as i32) * 1233 / 4096) as usize;
    t + usize::from(v >= POWERS_OF_TEN[t])
}

fn decimal_length64(v: u64) -> usize {
    const POWERS_OF_TEN: [u64; 20] = [
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000,
        10000000000,
        100000000000,
        1000000000000,
        10000000000000,
        100000000000000,
        1000000000000000,
        10000000000000000,
        100000000000000000,
        1000000000000000000,
        10000000000000000000,
    ];
    let t = ((64 - v.leading_zeros() as i32) * 1233 / 4096) as usize;
    t + usize::from(v >= POWERS_OF_TEN[t])
}

#[inline(always)]
unsafe fn put2(dst: *mut u8, table_idx: usize) {
    debug_assert!(table_idx < 199);
    // SAFETY: caller guarantees dst..dst+2 writable; table_idx <= 198.
    unsafe {
        core::ptr::copy_nonoverlapping(DIGIT_TABLE.as_ptr().add(table_idx), dst, 2);
    }
}

// C pg_ultoa_n: digit pairs blitted back-to-front; no NUL terminator.
pub fn pg_ultoa_n(mut value: u32, a: &mut [u8]) -> usize {
    if value == 0 {
        a[0] = b'0';
        return 1;
    }
    let olength = decimal_length32(value);
    assert!(a.len() >= olength);
    let p = a.as_mut_ptr();
    let mut i = 0usize;
    // SAFETY: every store lands in a[..olength]; olength <= a.len() asserted.
    unsafe {
        while value >= 10000 {
            let c = value % 10000;
            let c0 = ((c % 100) << 1) as usize;
            let c1 = ((c / 100) << 1) as usize;
            let pos = p.add(olength - i);
            value /= 10000;
            put2(pos.sub(2), c0);
            put2(pos.sub(4), c1);
            i += 4;
        }
        if value >= 100 {
            let c = ((value % 100) << 1) as usize;
            let pos = p.add(olength - i);
            value /= 100;
            put2(pos.sub(2), c);
            i += 2;
        }
        if value >= 10 {
            let c = (value << 1) as usize;
            let pos = p.add(olength - i);
            put2(pos.sub(2), c);
        } else {
            *p = b'0' + value as u8;
        }
    }
    olength
}

pub fn pg_ulltoa_n(mut value: u64, a: &mut [u8]) -> usize {
    if value == 0 {
        a[0] = b'0';
        return 1;
    }
    let olength = decimal_length64(value);
    assert!(a.len() >= olength);
    let p = a.as_mut_ptr();
    let mut i = 0usize;
    // SAFETY: every store lands in a[..olength]; olength <= a.len() asserted.
    unsafe {
        while value >= 100000000 {
            let q = value / 100000000;
            let value3 = (value - 100000000 * q) as u32;
            let c = value3 % 10000;
            let d = value3 / 10000;
            let c0 = ((c % 100) << 1) as usize;
            let c1 = ((c / 100) << 1) as usize;
            let d0 = ((d % 100) << 1) as usize;
            let d1 = ((d / 100) << 1) as usize;
            let pos = p.add(olength - i);
            value = q;
            put2(pos.sub(2), c0);
            put2(pos.sub(4), c1);
            put2(pos.sub(6), d0);
            put2(pos.sub(8), d1);
            i += 8;
        }
        let mut value2 = value as u32;
        if value2 >= 10000 {
            let c = value2 % 10000;
            let c0 = ((c % 100) << 1) as usize;
            let c1 = ((c / 100) << 1) as usize;
            let pos = p.add(olength - i);
            value2 /= 10000;
            put2(pos.sub(2), c0);
            put2(pos.sub(4), c1);
            i += 4;
        }
        if value2 >= 100 {
            let c = ((value2 % 100) << 1) as usize;
            let pos = p.add(olength - i);
            value2 /= 100;
            put2(pos.sub(2), c);
            i += 2;
        }
        if value2 >= 10 {
            let c = (value2 << 1) as usize;
            let pos = p.add(olength - i);
            put2(pos.sub(2), c);
        } else {
            *p = b'0' + value2 as u8;
        }
    }
    olength
}

// C pg_ltoa minus the trailing NUL (output encodes straight into the caller's
// buffer; no cstring round trip).
pub fn pg_ltoa(value: i32, a: &mut [u8]) -> usize {
    let mut len = 0usize;
    let uvalue = if value < 0 {
        a[0] = b'-';
        len = 1;
        0u32.wrapping_sub(value as u32)
    } else {
        value as u32
    };
    len + pg_ultoa_n(uvalue, &mut a[len..])
}

pub fn pg_lltoa(value: i64, a: &mut [u8]) -> usize {
    let mut len = 0usize;
    let uvalue = if value < 0 {
        a[0] = b'-';
        len = 1;
        0u64.wrapping_sub(value as u64)
    } else {
        value as u64
    };
    len + pg_ulltoa_n(uvalue, &mut a[len..])
}

pub fn pg_itoa(i: i16, a: &mut [u8]) -> usize {
    pg_ltoa(i32::from(i), a)
}

// Returns bytes written (C's end pointer as an offset); no NUL terminator.
pub fn pg_ultostr_zeropad(a: &mut [u8], value: u32, minwidth: i32) -> usize {
    assert!(minwidth > 0);
    let minwidth = minwidth as usize;

    if value < 100 && minwidth == 2 {
        let idx = value as usize * 2;
        assert!(a.len() >= 2);
        // SAFETY: idx <= 198, a[..2] writable per assert.
        unsafe { put2(a.as_mut_ptr(), idx) };
        return 2;
    }

    let len = pg_ultoa_n(value, a);
    if len >= minwidth {
        return len;
    }
    a.copy_within(..len, minwidth - len);
    a[..minwidth - len].fill(b'0');
    minwidth
}

pub fn pg_ultostr(a: &mut [u8], value: u32) -> usize {
    pg_ultoa_n(value, a)
}
