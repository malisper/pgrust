//! oracle_compat.c: lower/upper/initcap/casefold (case kernels in
//! [`casemap`]), lpad/rpad, dotrim + text/bytea trim family, translate,
//! ascii, chr, repeat; plus varlena.c's text_left/text_right/text_reverse
//! (the character-counting surface). Value cores over detoasted payload
//! slices, full
//! 4B-header [`Varlena`] images out built in one pass (single reserve +
//! byte-copy appends). btrim1/ltrim1/rtrim1 are the fixed-' ' one-arg SQL
//! forms.

pub mod builtins;
pub mod casemap;
#[cfg(test)]
mod tests;

use datum::{Bytea, Varlena};
use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

pub const VARHDRSZ: usize = datum::varlena::VARHDRSZ;

#[cold]
#[inline(never)]
fn length_too_large() -> PgError {
    PgError::error("requested length too large").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

#[cold]
#[inline(never)]
fn character_too_large() -> PgError {
    PgError::error("requested character too large").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

// CHECK_FOR_INTERRUPTS() (miscadmin.h): InterruptPending pre-check, then
// ProcessInterrupts through the tcop seam (ported since this loud stub was
// written; the stub turned any pending interrupt during a long repeat()
// into a panic — same class as the heaptoast site that killed
// vacuum-morsels battery rounds, notes/vacuum-morsels.md). The pre-check
// keeps seamless contexts (pure unit tests) off the seam.
fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

// C: max_length*nchars + VARHDRSZ, s32 overflow + AllocSizeIsValid -> 54000.
fn worst_case_bytelen(nchars: i32) -> PgResult<usize> {
    let bytelen = mbutils::pg_database_encoding_max_length()
        .checked_mul(nchars)
        .and_then(|b| b.checked_add(VARHDRSZ as i32));
    match bytelen {
        Some(b) if b as usize <= mcx::MAX_ALLOC_SIZE => Ok(b as usize),
        _ => Err(length_too_large().into()),
    }
}

// Encoding resolved once per call: the per-char TLS read (C: one global
// load) measured as real overhead; overrun cold path defers to mbutils.
#[derive(Clone, Copy)]
struct MbWalk {
    enc: i32,
}

impl MbWalk {
    fn new() -> Self {
        MbWalk {
            enc: mbutils::GetDatabaseEncoding(),
        }
    }

    #[inline]
    fn mblen_range(self, s: &[u8], pos: usize) -> PgResult<usize> {
        let l = wchar::pg_encoding_mblen(self.enc, &s[pos..]) as usize;
        if pos + l > s.len() {
            return Err(mbutils::pg_mblen_range(&s[pos..]).unwrap_err());
        }
        Ok(l)
    }

    #[inline]
    fn mblen(self, s: &[u8], pos: usize) -> usize {
        wchar::pg_encoding_mblen(self.enc, &s[pos..]) as usize
    }
}

fn image_with_capacity<'mcx>(mcx: Mcx<'mcx>, cap: usize) -> PgResult<PgVec<'mcx, u8>> {
    let mut image = mcx::vec_with_capacity_in(mcx, cap)?;
    mcx::vec_append_bytes(&mut image, &[0u8; VARHDRSZ])?;
    Ok(image)
}

fn text_result<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Varlena<'mcx>> {
    let mut image = image_with_capacity(mcx, VARHDRSZ + payload.len())?;
    mcx::vec_append_bytes(&mut image, payload)?;
    Ok(Varlena::from_image(image))
}

fn case_result<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    collid: Oid,
    kernel: fn(Mcx<'mcx>, &[u8], Oid) -> PgResult<PgVec<'mcx, u8>>,
) -> PgResult<Varlena<'mcx>> {
    let mapped = kernel(mcx, s, collid)?;
    text_result(mcx, &mapped)
}

pub fn lower<'mcx>(mcx: Mcx<'mcx>, s: &[u8], collid: Oid) -> PgResult<Varlena<'mcx>> {
    case_result(mcx, s, collid, casemap::str_tolower)
}

pub fn upper<'mcx>(mcx: Mcx<'mcx>, s: &[u8], collid: Oid) -> PgResult<Varlena<'mcx>> {
    case_result(mcx, s, collid, casemap::str_toupper)
}

pub fn initcap<'mcx>(mcx: Mcx<'mcx>, s: &[u8], collid: Oid) -> PgResult<Varlena<'mcx>> {
    case_result(mcx, s, collid, casemap::str_initcap)
}

pub fn casefold<'mcx>(mcx: Mcx<'mcx>, s: &[u8], collid: Oid) -> PgResult<Varlena<'mcx>> {
    case_result(mcx, s, collid, casemap::str_casefold)
}

fn pad<'mcx>(
    mcx: Mcx<'mcx>,
    string1: &[u8],
    len: i32,
    string2: &[u8],
    left: bool,
) -> PgResult<Varlena<'mcx>> {
    // Negative len is silently taken as zero.
    let len = len.max(0);
    let mut s1len = mbutils::pg_mbstrlen_with_len(string1)?;
    if s1len > len {
        s1len = len;
    }
    let len = if string2.is_empty() { s1len } else { len };

    let bytelen = worst_case_bytelen(len)?;
    let mut image = image_with_capacity(mcx, bytelen)?;
    let mb = MbWalk::new();

    // C fills its worst-case palloc raw (checked appends measured 1.6x C):
    // bytelen bounds every write, <= len chars x max_length bytes.
    let copy_s1 = |image: &mut PgVec<'mcx, u8>| -> PgResult<()> {
        let mut o1 = 0usize;
        let mut n = s1len;
        while n > 0 && o1 < string1.len() {
            let mlen = mb.mblen(string1, o1);
            let end = (o1 + mlen).min(string1.len());
            let old = image.len();
            debug_assert!(old + (end - o1) <= image.capacity());
            // SAFETY: capacity = bytelen covers the worst case (above).
            unsafe {
                core::ptr::copy_nonoverlapping(
                    string1.as_ptr().add(o1),
                    image.as_mut_ptr().add(old),
                    end - o1,
                );
                image.set_len(old + (end - o1));
            }
            o1 = end;
            n -= 1;
        }
        Ok(())
    };
    let copy_pad = |image: &mut PgVec<'mcx, u8>| -> PgResult<()> {
        let mut m = len - s1len;
        let mut o2 = 0usize;
        while m > 0 {
            let mlen = mb.mblen_range(string2, o2)?;
            let old = image.len();
            debug_assert!(old + mlen <= image.capacity());
            // SAFETY: capacity = bytelen covers the worst case (above).
            unsafe {
                core::ptr::copy_nonoverlapping(
                    string2.as_ptr().add(o2),
                    image.as_mut_ptr().add(old),
                    mlen,
                );
                image.set_len(old + mlen);
            }
            o2 += mlen;
            if o2 == string2.len() {
                o2 = 0;
            }
            m -= 1;
        }
        Ok(())
    };

    if left {
        copy_pad(&mut image)?;
        copy_s1(&mut image)?;
    } else {
        copy_s1(&mut image)?;
        copy_pad(&mut image)?;
    }
    Ok(Varlena::from_image(image))
}

pub fn lpad<'mcx>(
    mcx: Mcx<'mcx>,
    string1: &[u8],
    len: i32,
    string2: &[u8],
) -> PgResult<Varlena<'mcx>> {
    pad(mcx, string1, len, string2, true)
}

pub fn rpad<'mcx>(
    mcx: Mcx<'mcx>,
    string1: &[u8],
    len: i32,
    string2: &[u8],
) -> PgResult<Varlena<'mcx>> {
    pad(mcx, string1, len, string2, false)
}

// C's dotrim minus the final cstring_to_text_with_len: the surviving window.
pub fn dotrim_slice<'a>(
    mcx: Mcx<'_>,
    string: &'a [u8],
    set: &[u8],
    doltrim: bool,
    dortrim: bool,
) -> PgResult<&'a [u8]> {
    let mut start = 0usize;
    let mut len = string.len();
    if len > 0 && !set.is_empty() {
        if mbutils::pg_database_encoding_max_length() > 1 {
            let mb = MbWalk::new();
            let mut stringchars: PgVec<'_, (u32, u32)> =
                mcx::vec_with_capacity_in(mcx, string.len())?;
            let mut p = 0usize;
            while p < string.len() {
                let mblen = mb.mblen_range(string, p)?;
                stringchars.push((p as u32, mblen as u32));
                p += mblen;
            }
            let mut setchars: PgVec<'_, (u32, u32)> = mcx::vec_with_capacity_in(mcx, set.len())?;
            let mut p = 0usize;
            while p < set.len() {
                let mblen = mb.mblen_range(set, p)?;
                setchars.push((p as u32, mblen as u32));
                p += mblen;
            }

            let in_set = |(off, mblen): (u32, u32)| {
                let ch = &string[off as usize..(off + mblen) as usize];
                setchars.iter().any(|&(soff, slen)| {
                    slen == mblen && &set[soff as usize..(soff + slen) as usize] == ch
                })
            };

            let mut resultndx = 0usize;
            let mut resultnchars = stringchars.len();
            if doltrim {
                while resultnchars > 0 && in_set(stringchars[resultndx]) {
                    let (_, mblen) = stringchars[resultndx];
                    start += mblen as usize;
                    len -= mblen as usize;
                    resultndx += 1;
                    resultnchars -= 1;
                }
            }
            if dortrim {
                while resultnchars > 0 && in_set(stringchars[resultndx + resultnchars - 1]) {
                    let (_, mblen) = stringchars[resultndx + resultnchars - 1];
                    len -= mblen as usize;
                    resultnchars -= 1;
                }
            }
        } else {
            if doltrim {
                while len > 0 && set.contains(&string[start]) {
                    start += 1;
                    len -= 1;
                }
            }
            if dortrim {
                while len > 0 && set.contains(&string[start + len - 1]) {
                    len -= 1;
                }
            }
        }
    }
    Ok(&string[start..start + len])
}

pub fn dotrim<'mcx>(
    mcx: Mcx<'mcx>,
    string: &[u8],
    set: &[u8],
    doltrim: bool,
    dortrim: bool,
) -> PgResult<Varlena<'mcx>> {
    let window = dotrim_slice(mcx, string, set, doltrim, dortrim)?;
    text_result(mcx, window)
}

pub fn btrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<Varlena<'mcx>> {
    dotrim(mcx, string, set, true, true)
}

pub fn btrim1<'mcx>(mcx: Mcx<'mcx>, string: &[u8]) -> PgResult<Varlena<'mcx>> {
    dotrim(mcx, string, b" ", true, true)
}

pub fn ltrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<Varlena<'mcx>> {
    dotrim(mcx, string, set, true, false)
}

pub fn ltrim1<'mcx>(mcx: Mcx<'mcx>, string: &[u8]) -> PgResult<Varlena<'mcx>> {
    dotrim(mcx, string, b" ", true, false)
}

pub fn rtrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<Varlena<'mcx>> {
    dotrim(mcx, string, set, false, true)
}

pub fn rtrim1<'mcx>(mcx: Mcx<'mcx>, string: &[u8]) -> PgResult<Varlena<'mcx>> {
    dotrim(mcx, string, b" ", false, true)
}

// C returns the input bytea untouched when either side is empty; the
// surviving window keeps that zero-copy contract for the caller.
pub fn dobyteatrim<'a>(string: &'a [u8], set: &[u8], doltrim: bool, dortrim: bool) -> &'a [u8] {
    if string.is_empty() || set.is_empty() {
        return string;
    }
    let mut start = 0usize;
    let mut m = string.len();
    if doltrim {
        while m > 0 && set.contains(&string[start]) {
            start += 1;
            m -= 1;
        }
    }
    if dortrim {
        while m > 0 && set.contains(&string[start + m - 1]) {
            m -= 1;
        }
    }
    &string[start..start + m]
}

pub fn byteatrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<Bytea<'mcx>> {
    text_result(mcx, dobyteatrim(string, set, true, true))
}

pub fn bytealtrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<Bytea<'mcx>> {
    text_result(mcx, dobyteatrim(string, set, true, false))
}

pub fn byteartrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<Bytea<'mcx>> {
    text_result(mcx, dobyteatrim(string, set, false, true))
}

pub fn translate<'mcx>(
    mcx: Mcx<'mcx>,
    string: &[u8],
    from: &[u8],
    to: &[u8],
) -> PgResult<Varlena<'mcx>> {
    if string.is_empty() {
        return text_result(mcx, string);
    }
    let bytelen = worst_case_bytelen(string.len() as i32)?;
    let mut image = image_with_capacity(mcx, bytelen)?;
    let mb = MbWalk::new();

    // Raw worst-case fill (the lpad shape): one char per source char.
    let emit = |image: &mut PgVec<'mcx, u8>, bytes: &[u8]| {
        let old = image.len();
        debug_assert!(old + bytes.len() <= image.capacity());
        // SAFETY: capacity = bytelen covers the worst case (above).
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), image.as_mut_ptr().add(old), bytes.len());
            image.set_len(old + bytes.len());
        }
    };

    let mut src = 0usize;
    while src < string.len() {
        let source_len = mb.mblen_range(string, src)?;
        let source = &string[src..src + source_len];

        let mut from_index = 0usize;
        let mut i = 0usize;
        let mut matched = false;
        while i < from.len() {
            let flen = mb.mblen_range(from, i)?;
            if flen == source_len && &from[i..i + flen] == source {
                matched = true;
                break;
            }
            from_index += 1;
            i += flen;
        }
        if matched {
            // substitute, or delete if no corresponding "to" character
            let mut p = 0usize;
            let mut in_range = true;
            for _ in 0..from_index {
                if p >= to.len() {
                    in_range = false;
                    break;
                }
                p += mb.mblen_range(to, p)?;
            }
            if in_range && p < to.len() {
                let tlen = mb.mblen_range(to, p)?;
                emit(&mut image, &to[p..p + tlen]);
            }
        } else {
            emit(&mut image, source);
        }
        src += source_len;
    }
    Ok(Varlena::from_image(image))
}

pub fn ascii(string: &[u8]) -> PgResult<i32> {
    let encoding = mbutils::GetDatabaseEncoding();
    let Some(&b0) = string.first() else {
        return Ok(0);
    };

    if encoding == wchar::PG_UTF8 && b0 > 127 {
        let (mut result, tbytes) = if b0 >= 0xF0 {
            ((b0 & 0x07) as i32, 3)
        } else if b0 >= 0xE0 {
            ((b0 & 0x0F) as i32, 2)
        } else {
            debug_assert!(b0 > 0xC0);
            ((b0 & 0x1F) as i32, 1)
        };
        for i in 1..=tbytes {
            let b = string[i];
            debug_assert!(b & 0xC0 == 0x80);
            result = (result << 6) + (b & 0x3F) as i32;
        }
        Ok(result)
    } else {
        if wchar::pg_encoding_max_length(encoding) > 1 && b0 > 127 {
            return Err(character_too_large().into());
        }
        Ok(b0 as i32)
    }
}

pub fn chr<'mcx>(mcx: Mcx<'mcx>, arg: i32) -> PgResult<Varlena<'mcx>> {
    let encoding = mbutils::GetDatabaseEncoding();
    if arg < 0 {
        return Err(chr_not_positive_err().into());
    }
    if arg == 0 {
        return Err(chr_nul_err().into());
    }
    let cvalue = arg as u32;

    if encoding == wchar::PG_UTF8 && cvalue > 127 {
        // RFC3629: valid code points stop at U+10FFFF.
        if cvalue > 0x0010_FFFF {
            return Err(chr_too_large_err(cvalue).into());
        }
        let mut wch = [0u8; 4];
        let bytes: usize = if cvalue > 0xFFFF {
            4
        } else if cvalue > 0x07FF {
            3
        } else {
            2
        };
        match bytes {
            2 => {
                wch[0] = 0xC0 | ((cvalue >> 6) & 0x1F) as u8;
                wch[1] = 0x80 | (cvalue & 0x3F) as u8;
            }
            3 => {
                wch[0] = 0xE0 | ((cvalue >> 12) & 0x0F) as u8;
                wch[1] = 0x80 | ((cvalue >> 6) & 0x3F) as u8;
                wch[2] = 0x80 | (cvalue & 0x3F) as u8;
            }
            _ => {
                wch[0] = 0xF0 | ((cvalue >> 18) & 0x07) as u8;
                wch[1] = 0x80 | ((cvalue >> 12) & 0x3F) as u8;
                wch[2] = 0x80 | ((cvalue >> 6) & 0x3F) as u8;
                wch[3] = 0x80 | (cvalue & 0x3F) as u8;
            }
        }
        // Surrogate-pair codes pass the range check but are not legal UTF8.
        if !wchar::pg_utf8_islegal(&wch[..bytes], bytes as i32) {
            return Err(chr_not_valid_err(cvalue).into());
        }
        text_result(mcx, &wch[..bytes])
    } else {
        let is_mb = wchar::pg_encoding_max_length(encoding) > 1;
        if (is_mb && cvalue > 127) || (!is_mb && cvalue > 255) {
            return Err(chr_too_large_err(cvalue).into());
        }
        text_result(mcx, &[cvalue as u8])
    }
}

#[cold]
#[inline(never)]
fn chr_too_large_err(cvalue: u32) -> PgError {
    PgError::error(format!(
        "requested character too large for encoding: {cvalue}"
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

#[cold]
#[inline(never)]
fn chr_not_valid_err(cvalue: u32) -> PgError {
    PgError::error(format!(
        "requested character not valid for encoding: {cvalue}"
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

#[cold]
#[inline(never)]
fn chr_not_positive_err() -> PgError {
    PgError::error("character number must be positive")
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

#[cold]
#[inline(never)]
fn chr_nul_err() -> PgError {
    PgError::error("null character not permitted").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

// varlena.c text_left, n >= 0 arm inlined from text_substring(str, 1, n,
// false) over an already-detoasted slice: S=1 kills the S1/E1 clamps and the
// slice math; 1+n overflow means "run to end of string" (not an error).
pub fn text_left<'mcx>(mcx: Mcx<'mcx>, t: &[u8], n: i32) -> PgResult<Varlena<'mcx>> {
    if n < 0 {
        let n = mbutils::pg_mbstrlen_with_len(t)? + n;
        let rlen = mbutils::pg_mbcharcliplen(t, t.len() as i32, n)?;
        return text_result(mcx, &t[..rlen as usize]);
    }
    if n.checked_add(1).is_none() {
        return text_result(mcx, t);
    }
    if mbutils::pg_database_encoding_max_length() == 1 {
        return text_result(mcx, &t[..(n as usize).min(t.len())]);
    }
    if n == 0 || t.is_empty() {
        return text_result(mcx, b"");
    }
    let rlen = mbutils::pg_mbcharcliplen(t, t.len() as i32, n)?;
    text_result(mcx, &t[..rlen as usize])
}

pub fn text_right<'mcx>(mcx: Mcx<'mcx>, t: &[u8], n: i32) -> PgResult<Varlena<'mcx>> {
    // C's `n = -n` wraps for INT32_MIN (stays negative, clips to offset 0).
    let n = if n < 0 {
        n.wrapping_neg()
    } else {
        mbutils::pg_mbstrlen_with_len(t)? - n
    };
    let off = mbutils::pg_mbcharcliplen(t, t.len() as i32, n)? as usize;
    text_result(mcx, &t[off..])
}

pub fn text_reverse<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<Varlena<'mcx>> {
    let len = t.len();
    let mut image = image_with_capacity(mcx, VARHDRSZ + len)?;
    let base = image.as_mut_ptr();
    if mbutils::pg_database_encoding_max_length() > 1 {
        let mb = MbWalk::new();
        let mut src = 0usize;
        let mut dst = len;
        while src < len {
            let sz = mb.mblen_range(t, src)?;
            dst -= sz;
            // SAFETY: mblen_range bounds src+sz <= len, so dst stays in
            // [0, len); char sizes sum to len, covering every payload byte
            // exactly once before set_len below.
            unsafe {
                core::ptr::copy_nonoverlapping(t.as_ptr().add(src), base.add(VARHDRSZ + dst), sz);
            }
            src += sz;
        }
    } else {
        for (i, &b) in t.iter().enumerate() {
            // SAFETY: VARHDRSZ + len <= capacity; index < len.
            unsafe {
                *base.add(VARHDRSZ + len - 1 - i) = b;
            }
        }
    }
    // SAFETY: header + all len payload bytes written above.
    unsafe {
        image.set_len(VARHDRSZ + len);
    }
    Ok(Varlena::from_image(image))
}

pub fn repeat<'mcx>(mcx: Mcx<'mcx>, string: &[u8], count: i32) -> PgResult<Varlena<'mcx>> {
    let count = count.max(0);
    let slen = string.len() as i32;
    let tlen = count
        .checked_mul(slen)
        .and_then(|t| t.checked_add(VARHDRSZ as i32));
    let Some(tlen) = tlen.filter(|&t| t as usize <= mcx::MAX_ALLOC_SIZE) else {
        return Err(length_too_large().into());
    };

    let mut image = image_with_capacity(mcx, tlen as usize)?;
    for _ in 0..count {
        mcx::vec_append_bytes(&mut image, string)?;
        check_for_interrupts()?;
    }
    Ok(Varlena::from_image(image))
}
