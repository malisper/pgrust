mod big5;

use big5::{BIG5toCNS, CNStoBIG5};

use crate::{
    is_highbit_set, ConvArgs, Dst, LCPRV2_B, LC_CNS11643_1, LC_CNS11643_2, LC_CNS11643_3,
    LC_CNS11643_4, LC_CNS11643_7, SS2,
};
use datum::Datum;
use mbutils::{
    check_encoding_conversion_args, report_invalid_encoding, report_untranslatable_char,
};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use wchar::{pg_encoding_verifymbchar, PG_BIG5, PG_EUC_TW, PG_MULE_INTERNAL};

unsafe fn euc_tw2big5(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let c1 = src[pos];
        if is_highbit_set(c1) {
            let l = pg_encoding_verifymbchar(PG_EUC_TW, &src[pos..]);
            if l < 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_TW, &src[pos..]));
            }
            let (lc, cns_buf) = if c1 == SS2 {
                let plane = src[pos + 1];
                let lc = if plane == 0xa1 {
                    LC_CNS11643_1
                } else if plane == 0xa2 {
                    LC_CNS11643_2
                } else {
                    plane - 0xa3 + LC_CNS11643_3
                };
                (lc, ((src[pos + 2] as u16) << 8) | src[pos + 3] as u16)
            } else {
                (LC_CNS11643_1, ((c1 as u16) << 8) | src[pos + 1] as u16)
            };
            let big5buf = CNStoBIG5(cns_buf, lc);
            if big5buf == 0 {
                if no_error {
                    break;
                }
                return Err(report_untranslatable_char(PG_EUC_TW, PG_BIG5, &src[pos..]));
            }
            unsafe {
                out.push((big5buf >> 8) as u8);
                out.push(big5buf as u8);
            }
            pos += l as usize;
        } else {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_TW, &src[pos..]));
            }
            unsafe { out.push(c1) };
            pos += 1;
        }
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn big52euc_tw(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let c1 = src[pos];
        if is_highbit_set(c1) {
            let l = pg_encoding_verifymbchar(PG_BIG5, &src[pos..]);
            if l < 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_BIG5, &src[pos..]));
            }
            let big5buf = ((c1 as u16) << 8) | src[pos + 1] as u16;
            let (cns_buf, lc) = BIG5toCNS(big5buf);
            if lc == LC_CNS11643_1 {
                unsafe {
                    out.push((cns_buf >> 8) as u8);
                    out.push(cns_buf as u8);
                }
            } else if lc == LC_CNS11643_2 {
                unsafe {
                    out.push(SS2);
                    out.push(0xa2);
                    out.push((cns_buf >> 8) as u8);
                    out.push(cns_buf as u8);
                }
            } else if (LC_CNS11643_3..=LC_CNS11643_7).contains(&lc) {
                unsafe {
                    out.push(SS2);
                    out.push(lc - LC_CNS11643_3 + 0xa3);
                    out.push((cns_buf >> 8) as u8);
                    out.push(cns_buf as u8);
                }
            } else {
                if no_error {
                    break;
                }
                return Err(report_untranslatable_char(PG_BIG5, PG_EUC_TW, &src[pos..]));
            }
            pos += l as usize;
        } else {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_BIG5, &src[pos..]));
            }
            unsafe { out.push(c1) };
            pos += 1;
        }
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn euc_tw2mic(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let c1 = src[pos];
        if is_highbit_set(c1) {
            let l = pg_encoding_verifymbchar(PG_EUC_TW, &src[pos..]);
            if l < 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_TW, &src[pos..]));
            }
            unsafe {
                if c1 == SS2 {
                    let plane = src[pos + 1];
                    if plane == 0xa1 {
                        out.push(LC_CNS11643_1);
                    } else if plane == 0xa2 {
                        out.push(LC_CNS11643_2);
                    } else {
                        out.push(LCPRV2_B);
                        out.push(plane - 0xa3 + LC_CNS11643_3);
                    }
                    out.push(src[pos + 2]);
                    out.push(src[pos + 3]);
                } else {
                    out.push(LC_CNS11643_1);
                    out.push(c1);
                    out.push(src[pos + 1]);
                }
            }
            pos += l as usize;
        } else {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_TW, &src[pos..]));
            }
            unsafe { out.push(c1) };
            pos += 1;
        }
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn mic2euc_tw(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
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
        if c1 == LC_CNS11643_1 {
            unsafe {
                out.push(src[pos + 1]);
                out.push(src[pos + 2]);
            }
        } else if c1 == LC_CNS11643_2 {
            unsafe {
                out.push(SS2);
                out.push(0xa2);
                out.push(src[pos + 1]);
                out.push(src[pos + 2]);
            }
        } else if c1 == LCPRV2_B && (LC_CNS11643_3..=LC_CNS11643_7).contains(&src[pos + 1]) {
            unsafe {
                out.push(SS2);
                out.push(src[pos + 1] - LC_CNS11643_3 + 0xa3);
                out.push(src[pos + 2]);
                out.push(src[pos + 3]);
            }
        } else {
            if no_error {
                break;
            }
            return Err(report_untranslatable_char(
                PG_MULE_INTERNAL,
                PG_EUC_TW,
                &src[pos..],
            ));
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn big52mic(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let c1 = src[pos];
        if !is_highbit_set(c1) {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_BIG5, &src[pos..]));
            }
            unsafe { out.push(c1) };
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_BIG5, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_BIG5, &src[pos..]));
        }
        let big5buf = ((c1 as u16) << 8) | src[pos + 1] as u16;
        let (cns_buf, lc) = BIG5toCNS(big5buf);
        if lc != 0 {
            unsafe {
                if lc == LC_CNS11643_3 || lc == LC_CNS11643_4 {
                    out.push(LCPRV2_B);
                }
                out.push(lc);
                out.push((cns_buf >> 8) as u8);
                out.push(cns_buf as u8);
            }
        } else {
            if no_error {
                break;
            }
            return Err(report_untranslatable_char(
                PG_BIG5,
                PG_MULE_INTERNAL,
                &src[pos..],
            ));
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn mic2big5(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
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
        if c1 == LC_CNS11643_1 || c1 == LC_CNS11643_2 || c1 == LCPRV2_B {
            let (plane, cns_buf) = if c1 == LCPRV2_B {
                (
                    src[pos + 1],
                    ((src[pos + 2] as u16) << 8) | src[pos + 3] as u16,
                )
            } else {
                (c1, ((src[pos + 1] as u16) << 8) | src[pos + 2] as u16)
            };
            let big5buf = CNStoBIG5(cns_buf, plane);
            if big5buf == 0 {
                if no_error {
                    break;
                }
                return Err(report_untranslatable_char(
                    PG_MULE_INTERNAL,
                    PG_BIG5,
                    &src[pos..],
                ));
            }
            unsafe {
                out.push((big5buf >> 8) as u8);
                out.push(big5buf as u8);
            }
        } else {
            if no_error {
                break;
            }
            return Err(report_untranslatable_char(
                PG_MULE_INTERNAL,
                PG_BIG5,
                &src[pos..],
            ));
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

fc!(fc_euc_tw_to_big5, euc_tw2big5, PG_EUC_TW, PG_BIG5);
fc!(fc_big5_to_euc_tw, big52euc_tw, PG_BIG5, PG_EUC_TW);
fc!(fc_euc_tw_to_mic, euc_tw2mic, PG_EUC_TW, PG_MULE_INTERNAL);
fc!(fc_mic_to_euc_tw, mic2euc_tw, PG_MULE_INTERNAL, PG_EUC_TW);
fc!(fc_big5_to_mic, big52mic, PG_BIG5, PG_MULE_INTERNAL);
fc!(fc_mic_to_big5, mic2big5, PG_MULE_INTERNAL, PG_BIG5);
