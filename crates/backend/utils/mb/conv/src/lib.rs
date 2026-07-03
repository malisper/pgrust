#![allow(non_snake_case)]

// conv.c radix-tree engine + the UTF8<->{LATIN1,WIN*,ISO8859*} conversion
// procs (utf8_and_iso8859_1.c, utf8_and_win.c, utf8_and_iso8859.c). The
// combined-character maps and mule/latin table helpers of conv.c are not here:
// no radix-family conversion uses them; they land with their consumers
// (cyrillic/EUC families), which panic loudly today.

mod tables;

use datum::Datum;
use mbutils::{
    check_encoding_conversion_args, report_invalid_encoding, report_untranslatable_char,
};
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use wchar::{
    pg_enc, pg_encoding_verifymbchar, pg_utf8_islegal, pg_utf_mblen, pg_valid_encoding,
    PG_ISO_8859_5, PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8, PG_LATIN1, PG_LATIN10, PG_LATIN2,
    PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6, PG_LATIN7, PG_LATIN8, PG_LATIN9, PG_UTF8,
    PG_WIN1250, PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255, PG_WIN1256,
    PG_WIN1257, PG_WIN1258, PG_WIN866, PG_WIN874,
};

pub struct PgMbRadixTree {
    pub chars16: &'static [u16],
    pub chars32: &'static [u32],
    pub b1root: u32,
    pub b1_lower: u8,
    pub b1_upper: u8,
    pub b2root: u32,
    pub b2_1_lower: u8,
    pub b2_1_upper: u8,
    pub b2_2_lower: u8,
    pub b2_2_upper: u8,
    pub b3root: u32,
    pub b3_1_lower: u8,
    pub b3_1_upper: u8,
    pub b3_2_lower: u8,
    pub b3_2_upper: u8,
    pub b3_3_lower: u8,
    pub b3_3_upper: u8,
    pub b4root: u32,
    pub b4_1_lower: u8,
    pub b4_1_upper: u8,
    pub b4_2_lower: u8,
    pub b4_2_upper: u8,
    pub b4_3_lower: u8,
    pub b4_3_upper: u8,
    pub b4_4_lower: u8,
    pub b4_4_upper: u8,
}

impl PgMbRadixTree {
    #[inline]
    fn get(&self, idx: u32) -> u32 {
        if !self.chars32.is_empty() {
            self.chars32[idx as usize]
        } else {
            self.chars16[idx as usize] as u32
        }
    }
}

fn pg_mb_radix_conv(rt: &PgMbRadixTree, l: i32, b1: u8, b2: u8, b3: u8, b4: u8) -> u32 {
    match l {
        4 => {
            if b1 < rt.b4_1_lower
                || b1 > rt.b4_1_upper
                || b2 < rt.b4_2_lower
                || b2 > rt.b4_2_upper
                || b3 < rt.b4_3_lower
                || b3 > rt.b4_3_upper
                || b4 < rt.b4_4_lower
                || b4 > rt.b4_4_upper
            {
                return 0;
            }
            let mut idx = rt.b4root;
            idx = rt.get(b1 as u32 + idx - rt.b4_1_lower as u32);
            idx = rt.get(b2 as u32 + idx - rt.b4_2_lower as u32);
            idx = rt.get(b3 as u32 + idx - rt.b4_3_lower as u32);
            rt.get(b4 as u32 + idx - rt.b4_4_lower as u32)
        }
        3 => {
            if b2 < rt.b3_1_lower
                || b2 > rt.b3_1_upper
                || b3 < rt.b3_2_lower
                || b3 > rt.b3_2_upper
                || b4 < rt.b3_3_lower
                || b4 > rt.b3_3_upper
            {
                return 0;
            }
            let mut idx = rt.b3root;
            idx = rt.get(b2 as u32 + idx - rt.b3_1_lower as u32);
            idx = rt.get(b3 as u32 + idx - rt.b3_2_lower as u32);
            rt.get(b4 as u32 + idx - rt.b3_3_lower as u32)
        }
        2 => {
            if b3 < rt.b2_1_lower || b3 > rt.b2_1_upper || b4 < rt.b2_2_lower || b4 > rt.b2_2_upper
            {
                return 0;
            }
            let mut idx = rt.b2root;
            idx = rt.get(b3 as u32 + idx - rt.b2_1_lower as u32);
            rt.get(b4 as u32 + idx - rt.b2_2_lower as u32)
        }
        1 => {
            if b4 < rt.b1_lower || b4 > rt.b1_upper {
                return 0;
            }
            rt.get(b4 as u32 + rt.b1root - rt.b1_lower as u32)
        }
        _ => 0,
    }
}

/// Raw output cursor over the caller's conversion buffer.
struct Dst(*mut u8);

impl Dst {
    /// SAFETY: caller guarantees the mbutils conversion-buffer contract
    /// (capacity >= srclen * MAX_CONVERSION_GROWTH + 1); each source byte
    /// emits at most 4 output bytes plus one final NUL.
    #[inline]
    unsafe fn push(&mut self, b: u8) {
        unsafe {
            *self.0 = b;
            self.0 = self.0.add(1);
        }
    }

    unsafe fn store_coded_char(&mut self, code: u32) {
        unsafe {
            if code & 0xff00_0000 != 0 {
                self.push((code >> 24) as u8);
            }
            if code & 0x00ff_0000 != 0 {
                self.push((code >> 16) as u8);
            }
            if code & 0x0000_ff00 != 0 {
                self.push((code >> 8) as u8);
            }
            if code & 0x0000_00ff != 0 {
                self.push(code as u8);
            }
        }
    }
}

#[cold]
fn invalid_encoding_number(encoding: pg_enc) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid encoding number: {encoding}"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

/// C `UtfToLocal` with `cmap`/`conv_func` absent (all radix-family callers
/// pass NULL for both). Writes NUL-terminated bytes through `dest`; returns
/// source bytes consumed (< `src.len()` only when `no_error`).
///
/// # Safety
/// `dest` must satisfy the mbutils conversion-buffer contract:
/// `src.len() * MAX_CONVERSION_GROWTH + 1` writable bytes.
pub unsafe fn UtfToLocal(
    src: &[u8],
    dest: *mut u8,
    map: &PgMbRadixTree,
    encoding: pg_enc,
    no_error: bool,
) -> PgResult<i32> {
    if !pg_valid_encoding(encoding) {
        return Err(invalid_encoding_number(encoding));
    }
    let mut out = Dst(dest);
    let mut pos = 0usize;
    loop {
        let remaining = src.len() - pos;
        if remaining == 0 || src[pos] == 0 {
            break;
        }
        let l = pg_utf_mblen(&src[pos..]);
        if remaining < l as usize {
            break;
        }
        if !pg_utf8_islegal(&src[pos..], l) {
            break;
        }
        if l == 1 {
            unsafe { out.push(src[pos]) };
            pos += 1;
            continue;
        }
        let (b1, b2, b3, b4) = match l {
            2 => (0, 0, src[pos], src[pos + 1]),
            3 => (0, src[pos], src[pos + 1], src[pos + 2]),
            _ => (src[pos], src[pos + 1], src[pos + 2], src[pos + 3]),
        };
        let converted = pg_mb_radix_conv(map, l, b1, b2, b3, b4);
        if converted != 0 {
            unsafe { out.store_coded_char(converted) };
            pos += l as usize;
            continue;
        }
        if no_error {
            break;
        }
        return Err(report_untranslatable_char(PG_UTF8, encoding, &src[pos..]));
    }
    if pos < src.len() && !no_error {
        return Err(report_invalid_encoding(PG_UTF8, &src[pos..]));
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

/// C `LocalToUtf` with `cmap`/`conv_func` absent; see [`UtfToLocal`].
///
/// # Safety
/// Same `dest` contract as [`UtfToLocal`].
pub unsafe fn LocalToUtf(
    src: &[u8],
    dest: *mut u8,
    map: &PgMbRadixTree,
    encoding: pg_enc,
    no_error: bool,
) -> PgResult<i32> {
    if !pg_valid_encoding(encoding) {
        return Err(invalid_encoding_number(encoding));
    }
    let mut out = Dst(dest);
    let mut pos = 0usize;
    loop {
        let remaining = src.len() - pos;
        if remaining == 0 || src[pos] == 0 {
            break;
        }
        if src[pos] & 0x80 == 0 {
            unsafe { out.push(src[pos]) };
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(encoding, &src[pos..]);
        if l < 0 {
            break;
        }
        let (b1, b2, b3, b4) = match l {
            1 => (0, 0, 0, src[pos]),
            2 => (0, 0, src[pos], src[pos + 1]),
            3 => (0, src[pos], src[pos + 1], src[pos + 2]),
            _ => (src[pos], src[pos + 1], src[pos + 2], src[pos + 3]),
        };
        let converted = pg_mb_radix_conv(map, l, b1, b2, b3, b4);
        if converted != 0 {
            unsafe { out.store_coded_char(converted) };
            pos += l as usize;
            continue;
        }
        if no_error {
            break;
        }
        return Err(report_untranslatable_char(encoding, PG_UTF8, &src[pos..]));
    }
    if pos < src.len() && !no_error {
        return Err(report_invalid_encoding(encoding, &src[pos..]));
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

struct ConvArgs {
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src_ptr: *const u8,
    dest: *mut u8,
    len: i32,
    no_error: bool,
}

impl ConvArgs {
    /// SAFETY: callers arrive only through the conversion-proc fcinfo contract
    /// (mbutils convert_with_proc / C FunctionCall6): arg2 = live source
    /// pointer readable for arg4 bytes, arg3 = destination buffer of
    /// len * MAX_CONVERSION_GROWTH + 1 writable bytes, len >= 0.
    unsafe fn from(fcinfo: &Fcinfo) -> Self {
        unsafe {
            ConvArgs {
                src_encoding: fcinfo.arg_i32(0),
                dest_encoding: fcinfo.arg_i32(1),
                src_ptr: fcinfo.arg_ptr(2),
                dest: fcinfo.arg_ptr(3) as *mut u8,
                len: fcinfo.arg_i32(4),
                no_error: fcinfo.arg_bool(5),
            }
        }
    }

    fn src(&self) -> &[u8] {
        // SAFETY: see from(); check_encoding_conversion_args rejected len < 0.
        unsafe { core::slice::from_raw_parts(self.src_ptr, self.len as usize) }
    }
}

// utf8_and_iso8859_1.c: LATIN1 is algorithmic (Unicode 0x80..=0xFF), no map.
pub fn fc_iso8859_1_to_utf8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, PG_LATIN1, PG_UTF8)?;
    let src = a.src();
    let mut out = Dst(a.dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let c = src[pos];
        if c == 0 {
            if a.no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_LATIN1, &src[pos..]));
        }
        // SAFETY: Dst buffer contract (ConvArgs::from); <=2 bytes per input byte.
        unsafe {
            if c & 0x80 == 0 {
                out.push(c);
            } else {
                out.push((c >> 6) | 0xc0);
                out.push((c & 0x3f) | 0x80);
            }
        }
        pos += 1;
    }
    unsafe { *out.0 = 0 };
    Ok(Datum::from_i32(pos as i32))
}

pub fn fc_utf8_to_iso8859_1(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, PG_UTF8, PG_LATIN1)?;
    let src = a.src();
    let mut out = Dst(a.dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let remaining = src.len() - pos;
        let c = src[pos];
        if c == 0 {
            if a.no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_UTF8, &src[pos..]));
        }
        if c & 0x80 == 0 {
            unsafe { out.push(c) };
            pos += 1;
            continue;
        }
        let l = pg_utf_mblen(&src[pos..]);
        if l as usize > remaining || !pg_utf8_islegal(&src[pos..], l) {
            if a.no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_UTF8, &src[pos..]));
        }
        if l != 2 {
            if a.no_error {
                break;
            }
            return Err(report_untranslatable_char(PG_UTF8, PG_LATIN1, &src[pos..]));
        }
        let c1 = (src[pos + 1] & 0x3f) as u16;
        let code = (((c as u16) & 0x1f) << 6) | c1;
        if (0x80..=0xff).contains(&code) {
            unsafe { out.push(code as u8) };
            pos += 2;
        } else {
            if a.no_error {
                break;
            }
            return Err(report_untranslatable_char(PG_UTF8, PG_LATIN1, &src[pos..]));
        }
    }
    unsafe { *out.0 = 0 };
    Ok(Datum::from_i32(pos as i32))
}

fn win_maps(encoding: pg_enc) -> Option<(&'static PgMbRadixTree, &'static PgMbRadixTree)> {
    match encoding {
        PG_WIN1252 => Some((
            &tables::WIN1252_TO_UNICODE_TREE,
            &tables::WIN1252_FROM_UNICODE_TREE,
        )),
        _ => None,
    }
}

fn iso8859_maps(encoding: pg_enc) -> Option<(&'static PgMbRadixTree, &'static PgMbRadixTree)> {
    match encoding {
        PG_LATIN9 => Some((
            &tables::ISO8859_15_TO_UNICODE_TREE,
            &tables::ISO8859_15_FROM_UNICODE_TREE,
        )),
        _ => None,
    }
}

const WIN_FAMILY: [pg_enc; 11] = [
    PG_WIN866, PG_WIN874, PG_WIN1250, PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255,
    PG_WIN1256, PG_WIN1257, PG_WIN1258,
];

const ISO8859_FAMILY: [pg_enc; 13] = [
    PG_LATIN2,
    PG_LATIN3,
    PG_LATIN4,
    PG_LATIN5,
    PG_LATIN6,
    PG_LATIN7,
    PG_LATIN8,
    PG_LATIN9,
    PG_LATIN10,
    PG_ISO_8859_5,
    PG_ISO_8859_6,
    PG_ISO_8859_7,
    PG_ISO_8859_8,
];

#[cold]
fn unexpected_encoding(encoding: pg_enc, family: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "unexpected encoding ID {encoding} for {family} character sets"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

fn family_maps(
    encoding: pg_enc,
    family: &'static [pg_enc],
    family_name: &'static str,
    lookup: fn(pg_enc) -> Option<(&'static PgMbRadixTree, &'static PgMbRadixTree)>,
) -> PgResult<(&'static PgMbRadixTree, &'static PgMbRadixTree)> {
    match lookup(encoding) {
        Some(maps) => Ok(maps),
        None if family.contains(&encoding) => panic!(
            "conv: {family_name} radix tables for encoding {encoding} not ported (only WIN1252/LATIN9 are)"
        ),
        None => Err(unexpected_encoding(encoding, family_name)),
    }
}

pub fn fc_win_to_utf8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, -1, PG_UTF8)?;
    let (to_utf8, _) = family_maps(a.src_encoding, &WIN_FAMILY, "WIN", win_maps)?;
    let n = unsafe { LocalToUtf(a.src(), a.dest, to_utf8, a.src_encoding, a.no_error)? };
    Ok(Datum::from_i32(n))
}

pub fn fc_utf8_to_win(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, PG_UTF8, -1)?;
    let (_, from_utf8) = family_maps(a.dest_encoding, &WIN_FAMILY, "WIN", win_maps)?;
    let n = unsafe { UtfToLocal(a.src(), a.dest, from_utf8, a.dest_encoding, a.no_error)? };
    Ok(Datum::from_i32(n))
}

pub fn fc_iso8859_to_utf8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, -1, PG_UTF8)?;
    let (to_utf8, _) = family_maps(a.src_encoding, &ISO8859_FAMILY, "ISO 8859", iso8859_maps)?;
    let n = unsafe { LocalToUtf(a.src(), a.dest, to_utf8, a.src_encoding, a.no_error)? };
    Ok(Datum::from_i32(n))
}

pub fn fc_utf8_to_iso8859(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, PG_UTF8, -1)?;
    let (_, from_utf8) = family_maps(a.dest_encoding, &ISO8859_FAMILY, "ISO 8859", iso8859_maps)?;
    let n = unsafe { UtfToLocal(a.src(), a.dest, from_utf8, a.dest_encoding, a.no_error)? };
    Ok(Datum::from_i32(n))
}

// pg_proc rows for these are prolang 'c' (not in fmgrtab/CANONICAL); OIDs
// verified against pg_proc.dat 18.3. Strictly OID-ascending.
pub const CONV_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 4358,
        name: "utf8_to_win",
        nargs: 6,
        strict: true,
        retset: false,
        func: fc_utf8_to_win,
    },
    FmgrBuiltin {
        foid: 4359,
        name: "win_to_utf8",
        nargs: 6,
        strict: true,
        retset: false,
        func: fc_win_to_utf8,
    },
    FmgrBuiltin {
        foid: 4372,
        name: "utf8_to_iso8859",
        nargs: 6,
        strict: true,
        retset: false,
        func: fc_utf8_to_iso8859,
    },
    FmgrBuiltin {
        foid: 4373,
        name: "iso8859_to_utf8",
        nargs: 6,
        strict: true,
        retset: false,
        func: fc_iso8859_to_utf8,
    },
    FmgrBuiltin {
        foid: 4374,
        name: "iso8859_1_to_utf8",
        nargs: 6,
        strict: true,
        retset: false,
        func: fc_iso8859_1_to_utf8,
    },
    FmgrBuiltin {
        foid: 4375,
        name: "utf8_to_iso8859_1",
        nargs: 6,
        strict: true,
        retset: false,
        func: fc_utf8_to_iso8859_1,
    },
];

pub fn conv_builtin(oid: Oid) -> Option<&'static FmgrBuiltin> {
    CONV_BUILTINS
        .binary_search_by_key(&oid, |b| b.foid)
        .ok()
        .map(|i| &CONV_BUILTINS[i])
}

#[cfg(test)]
mod tests;
