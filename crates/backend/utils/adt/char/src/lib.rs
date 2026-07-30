#![allow(clippy::result_large_err)]

pub mod builtins;
#[cfg(test)]
mod tests;

use ::datum::{Bytea, Varlena};
use ::mcx::Mcx;
use ::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgbyte, pq_sendbyte};
use ::stringinfo::StringInfo;
use ::types_error::{PgError, PgResult, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};

fn is_octal(c: u8) -> bool {
    (b'0'..=b'7').contains(&c)
}

// C sums the escape in int then truncates on the char assignment, so a
// leading digit > 3 wraps modulo 256 exactly as the `as u8` does.
fn decode_octal(o1: u8, o2: u8, o3: u8) -> u8 {
    ((((o1 - b'0') as u32) << 6) + (((o2 - b'0') as u32) << 3) + (o3 - b'0') as u32) as u8
}

pub fn charin(ch: &[u8]) -> i8 {
    if ch.len() == 4 && ch[0] == b'\\' && is_octal(ch[1]) && is_octal(ch[2]) && is_octal(ch[3]) {
        return decode_octal(ch[1], ch[2], ch[3]) as i8;
    }
    // Zero-length input reads C's terminating NUL as ch[0].
    ch.first().copied().unwrap_or(0) as i8
}

// Image: 0x00 -> empty, ASCII -> the byte, high-bit set -> \ooo escape.
pub fn charout(ch: i8, buf: &mut [u8; 4]) -> usize {
    let ch = ch as u8;
    if ch & 0x80 != 0 {
        buf[0] = b'\\';
        buf[1] = (ch >> 6) + b'0';
        buf[2] = ((ch >> 3) & 0o7) + b'0';
        buf[3] = (ch & 0o7) + b'0';
        4
    } else if ch != 0 {
        buf[0] = ch;
        1
    } else {
        0
    }
}

pub fn charrecv(buf: &mut StringInfo<'_>) -> PgResult<i8> {
    Ok(pq_getmsgbyte(buf)? as i8)
}

pub fn charsend(mcx: Mcx<'_>, arg1: i8) -> PgResult<Bytea<'_>> {
    let mut buf = pq_begintypsend(mcx)?;
    pq_sendbyte(&mut buf, arg1 as u8)?;
    Ok(pq_endtypsend(buf))
}

// char.c:119: comparisons treat char as unsigned; int conversions as signed.
pub fn chareq(arg1: i8, arg2: i8) -> bool {
    arg1 == arg2
}

pub fn charne(arg1: i8, arg2: i8) -> bool {
    arg1 != arg2
}

pub fn charlt(arg1: i8, arg2: i8) -> bool {
    (arg1 as u8) < (arg2 as u8)
}

pub fn charle(arg1: i8, arg2: i8) -> bool {
    (arg1 as u8) <= (arg2 as u8)
}

pub fn chargt(arg1: i8, arg2: i8) -> bool {
    (arg1 as u8) > (arg2 as u8)
}

pub fn charge(arg1: i8, arg2: i8) -> bool {
    (arg1 as u8) >= (arg2 as u8)
}

pub fn chartoi4(arg1: i8) -> i32 {
    arg1 as i32
}

pub fn i4tochar(arg1: i32) -> PgResult<i8> {
    if !(i8::MIN as i32..=i8::MAX as i32).contains(&arg1) {
        return Err(out_of_range());
    }
    Ok(arg1 as i8)
}

#[track_caller]
#[cold]
#[inline(never)]
fn out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("\"char\" out of range".to_string())
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

pub fn text_char(payload: &[u8]) -> i8 {
    charin(payload)
}

pub fn char_text(mcx: Mcx<'_>, arg1: i8) -> PgResult<Varlena<'_>> {
    let mut buf = [0u8; 4];
    let n = charout(arg1, &mut buf);
    varlena::cstring_to_text(mcx, &buf[..n])
}
