use crate::maps::local_tables::IBMKANJI;
use crate::{is_highbit_set, ConvArgs, Dst, LC_JISX0201K, LC_JISX0208, LC_JISX0212, SS2, SS3};
use datum::Datum;
use mbutils::{
    check_encoding_conversion_args, report_invalid_encoding, report_untranslatable_char,
};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use wchar::{pg_encoding_verifymbchar, PG_EUC_JP, PG_MULE_INTERNAL, PG_SJIS};

const PGSJISALTCODE: i32 = 0x81ac;
const PGEUCALTCODE: i32 = 0xa2ae;

fn issjishead(c: i32) -> bool {
    (0x81..=0x9f).contains(&c) || (0xe0..=0xfc).contains(&c)
}

fn issjistail(c: i32) -> bool {
    (0x40..=0x7e).contains(&c) || (0x80..=0xfc).contains(&c)
}

unsafe fn sjis2mic(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let mut c1 = src[pos] as i32;
        if (0xa1..=0xdf).contains(&c1) {
            unsafe {
                out.push(LC_JISX0201K);
                out.push(c1 as u8);
            }
            pos += 1;
        } else if is_highbit_set(src[pos]) {
            if src.len() - pos < 2 || !issjishead(c1) || !issjistail(src[pos + 1] as i32) {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_SJIS, &src[pos..]));
            }
            let mut c2 = src[pos + 1] as i32;
            let mut k = (c1 << 8) + c2;
            if (0xed40..0xf040).contains(&k) {
                // NEC selection IBM kanji; sentinel-terminated scan, no break
                // on match (C shape).
                for e in IBMKANJI.iter() {
                    let k2 = e.nec as i32;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        k = e.sjis as i32;
                        c1 = (k >> 8) & 0xff;
                        c2 = k & 0xff;
                    }
                }
            }
            if k < 0xeb3f {
                unsafe {
                    out.push(LC_JISX0208);
                    out.push((((c1 & 0x3f) << 1) + 0x9f + (c2 > 0x9e) as i32) as u8);
                    out.push(
                        (c2 + if c2 > 0x9e { 2 } else { 0x60 } + (c2 < 0x80) as i32) as u8,
                    );
                }
            } else if (0xeb40..0xf040).contains(&k) || (0xfc4c..=0xfcfc).contains(&k) {
                unsafe {
                    out.push(LC_JISX0208);
                    out.push((PGEUCALTCODE >> 8) as u8);
                    out.push((PGEUCALTCODE & 0xff) as u8);
                }
            } else if (0xf040..0xf540).contains(&k) {
                unsafe {
                    out.push(LC_JISX0208);
                    c1 -= 0x6f;
                    out.push((((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e) as i32) as u8);
                    out.push(
                        (c2 + if c2 > 0x9e { 2 } else { 0x60 } + (c2 < 0x80) as i32) as u8,
                    );
                }
            } else if (0xf540..0xfa40).contains(&k) {
                unsafe {
                    out.push(LC_JISX0212);
                    c1 -= 0x74;
                    out.push((((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e) as i32) as u8);
                    out.push(
                        (c2 + if c2 > 0x9e { 2 } else { 0x60 } + (c2 < 0x80) as i32) as u8,
                    );
                }
            } else if k >= 0xfa40 {
                for e in IBMKANJI.iter() {
                    let k2 = e.sjis as i32;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        let k = e.euc;
                        unsafe {
                            if k >= 0x8f0000 {
                                out.push(LC_JISX0212);
                                out.push((0x80 | ((k & 0xff00) >> 8)) as u8);
                                out.push((0x80 | (k & 0xff)) as u8);
                            } else {
                                out.push(LC_JISX0208);
                                out.push((0x80 | (k >> 8)) as u8);
                                out.push((0x80 | (k & 0xff)) as u8);
                            }
                        }
                    }
                }
            }
            pos += 2;
        } else {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_SJIS, &src[pos..]));
            }
            unsafe { out.push(c1 as u8) };
            pos += 1;
        }
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn mic2sjis(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let mut c1 = src[pos] as i32;
        if !is_highbit_set(src[pos]) {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..]));
            }
            unsafe { out.push(c1 as u8) };
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_MULE_INTERNAL, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..]));
        }
        if c1 == LC_JISX0201K as i32 {
            unsafe { out.push(src[pos + 1]) };
        } else if c1 == LC_JISX0208 as i32 {
            c1 = src[pos + 1] as i32;
            let c2 = src[pos + 2] as i32;
            let k = (c1 << 8) | (c2 & 0xff);
            unsafe {
                if k >= 0xf5a1 {
                    c1 -= 0x54;
                    out.push(
                        (((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 } + 0x6f) as u8,
                    );
                } else {
                    out.push((((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 }) as u8);
                }
                out.push(
                    (c2 - if c1 & 1 != 0 {
                        if c2 < 0xe0 {
                            0x61
                        } else {
                            0x60
                        }
                    } else {
                        2
                    }) as u8,
                );
            }
        } else if c1 == LC_JISX0212 as i32 {
            c1 = src[pos + 1] as i32;
            let c2 = src[pos + 2] as i32;
            let k = (c1 << 8) | c2;
            if k >= 0xf5a1 {
                unsafe {
                    c1 -= 0x54;
                    out.push(
                        (((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 } + 0x74) as u8,
                    );
                    out.push(
                        (c2 - if c1 & 1 != 0 {
                            if c2 < 0xe0 {
                                0x61
                            } else {
                                0x60
                            }
                        } else {
                            2
                        }) as u8,
                    );
                }
            } else {
                for e in IBMKANJI.iter() {
                    let k2 = e.euc & 0xffff;
                    if k2 == 0xffff {
                        unsafe {
                            out.push((PGSJISALTCODE >> 8) as u8);
                            out.push((PGSJISALTCODE & 0xff) as u8);
                        }
                        break;
                    }
                    if k2 == k {
                        let k = e.sjis as i32;
                        unsafe {
                            out.push((k >> 8) as u8);
                            out.push((k & 0xff) as u8);
                        }
                        break;
                    }
                }
            }
        } else {
            if no_error {
                break;
            }
            return Err(report_untranslatable_char(
                PG_MULE_INTERNAL,
                PG_SJIS,
                &src[pos..],
            ));
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn euc_jp2mic(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let c1 = src[pos];
        if !is_highbit_set(c1) {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_JP, &src[pos..]));
            }
            unsafe { out.push(c1) };
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_EUC_JP, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_EUC_JP, &src[pos..]));
        }
        unsafe {
            if c1 == SS2 {
                out.push(LC_JISX0201K);
                out.push(src[pos + 1]);
            } else if c1 == SS3 {
                out.push(LC_JISX0212);
                out.push(src[pos + 1]);
                out.push(src[pos + 2]);
            } else {
                out.push(LC_JISX0208);
                out.push(c1);
                out.push(src[pos + 1]);
            }
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn mic2euc_jp(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let c1 = src[pos];
        if !is_highbit_set(c1) {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..]));
            }
            unsafe { out.push(c1) };
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_MULE_INTERNAL, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..]));
        }
        if c1 == LC_JISX0201K {
            unsafe {
                out.push(SS2);
                out.push(src[pos + 1]);
            }
        } else if c1 == LC_JISX0212 {
            unsafe {
                out.push(SS3);
                out.push(src[pos + 1]);
                out.push(src[pos + 2]);
            }
        } else if c1 == LC_JISX0208 {
            unsafe {
                out.push(src[pos + 1]);
                out.push(src[pos + 2]);
            }
        } else {
            if no_error {
                break;
            }
            return Err(report_untranslatable_char(
                PG_MULE_INTERNAL,
                PG_EUC_JP,
                &src[pos..],
            ));
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn euc_jp2sjis(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let mut c1 = src[pos] as i32;
        if !is_highbit_set(src[pos]) {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_JP, &src[pos..]));
            }
            unsafe { out.push(c1 as u8) };
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_EUC_JP, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_EUC_JP, &src[pos..]));
        }
        if c1 == SS2 as i32 {
            unsafe { out.push(src[pos + 1]) };
        } else if c1 == SS3 as i32 {
            c1 = src[pos + 1] as i32;
            let c2 = src[pos + 2] as i32;
            let k = (c1 << 8) | c2;
            if k >= 0xf5a1 {
                unsafe {
                    c1 -= 0x54;
                    out.push(
                        (((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 } + 0x74) as u8,
                    );
                    out.push(
                        (c2 - if c1 & 1 != 0 {
                            if c2 < 0xe0 {
                                0x61
                            } else {
                                0x60
                            }
                        } else {
                            2
                        }) as u8,
                    );
                }
            } else {
                for e in IBMKANJI.iter() {
                    let k2 = e.euc & 0xffff;
                    if k2 == 0xffff {
                        unsafe {
                            out.push((PGSJISALTCODE >> 8) as u8);
                            out.push((PGSJISALTCODE & 0xff) as u8);
                        }
                        break;
                    }
                    if k2 == k {
                        let k = e.sjis as i32;
                        unsafe {
                            out.push((k >> 8) as u8);
                            out.push((k & 0xff) as u8);
                        }
                        break;
                    }
                }
            }
        } else {
            let c2 = src[pos + 1] as i32;
            let k = (c1 << 8) | (c2 & 0xff);
            unsafe {
                if k >= 0xf5a1 {
                    c1 -= 0x54;
                    out.push(
                        (((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 } + 0x6f) as u8,
                    );
                } else {
                    out.push((((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 }) as u8);
                }
                out.push(
                    (c2 - if c1 & 1 != 0 {
                        if c2 < 0xe0 {
                            0x61
                        } else {
                            0x60
                        }
                    } else {
                        2
                    }) as u8,
                );
            }
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn sjis2euc_jp(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let mut c1 = src[pos] as i32;
        if !is_highbit_set(src[pos]) {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_SJIS, &src[pos..]));
            }
            unsafe { out.push(c1 as u8) };
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_SJIS, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_SJIS, &src[pos..]));
        }
        if (0xa1..=0xdf).contains(&c1) {
            unsafe {
                out.push(SS2);
                out.push(c1 as u8);
            }
        } else {
            let mut c2 = src[pos + 1] as i32;
            let mut k = (c1 << 8) + c2;
            if (0xed40..0xf040).contains(&k) {
                for e in IBMKANJI.iter() {
                    let k2 = e.nec as i32;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        k = e.sjis as i32;
                        c1 = (k >> 8) & 0xff;
                        c2 = k & 0xff;
                    }
                }
            }
            if k < 0xeb3f {
                unsafe {
                    out.push((((c1 & 0x3f) << 1) + 0x9f + (c2 > 0x9e) as i32) as u8);
                    out.push(
                        (c2 + if c2 > 0x9e { 2 } else { 0x60 } + (c2 < 0x80) as i32) as u8,
                    );
                }
            } else if (0xeb40..0xf040).contains(&k) || (0xfc4c..=0xfcfc).contains(&k) {
                unsafe {
                    out.push((PGEUCALTCODE >> 8) as u8);
                    out.push((PGEUCALTCODE & 0xff) as u8);
                }
            } else if (0xf040..0xf540).contains(&k) {
                unsafe {
                    c1 -= 0x6f;
                    out.push((((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e) as i32) as u8);
                    out.push(
                        (c2 + if c2 > 0x9e { 2 } else { 0x60 } + (c2 < 0x80) as i32) as u8,
                    );
                }
            } else if (0xf540..0xfa40).contains(&k) {
                unsafe {
                    out.push(SS3);
                    c1 -= 0x74;
                    out.push((((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e) as i32) as u8);
                    out.push(
                        (c2 + if c2 > 0x9e { 2 } else { 0x60 } + (c2 < 0x80) as i32) as u8,
                    );
                }
            } else if k >= 0xfa40 {
                for e in IBMKANJI.iter() {
                    let k2 = e.sjis as i32;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        let k = e.euc;
                        unsafe {
                            if k >= 0x8f0000 {
                                out.push(SS3);
                                out.push((0x80 | ((k & 0xff00) >> 8)) as u8);
                                out.push((0x80 | (k & 0xff)) as u8);
                            } else {
                                out.push((0x80 | (k >> 8)) as u8);
                                out.push((0x80 | (k & 0xff)) as u8);
                            }
                        }
                    }
                }
            }
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

macro_rules! fc {
    ($name:ident, $inner:ident, $src:expr, $dst:expr) => {
        pub fn $name(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = unsafe { ConvArgs::from(fcinfo) };
            check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, $src, $dst)?;
            let n = unsafe { $inner(a.src(), a.dest, a.no_error)? };
            Ok(Datum::from_i32(n))
        }
    };
}

fc!(fc_euc_jp_to_sjis, euc_jp2sjis, PG_EUC_JP, PG_SJIS);
fc!(fc_sjis_to_euc_jp, sjis2euc_jp, PG_SJIS, PG_EUC_JP);
fc!(fc_euc_jp_to_mic, euc_jp2mic, PG_EUC_JP, PG_MULE_INTERNAL);
fc!(fc_mic_to_euc_jp, mic2euc_jp, PG_MULE_INTERNAL, PG_EUC_JP);
fc!(fc_sjis_to_mic, sjis2mic, PG_SJIS, PG_MULE_INTERNAL);
fc!(fc_mic_to_sjis, mic2sjis, PG_MULE_INTERNAL, PG_SJIS);
