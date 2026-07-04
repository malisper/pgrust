use crate::{is_highbit_set, ConvArgs, Dst, LC_KS5601};
use datum::Datum;
use mbutils::{
    check_encoding_conversion_args, report_invalid_encoding, report_untranslatable_char,
};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use wchar::{pg_encoding_verifymbchar, PG_EUC_KR, PG_MULE_INTERNAL};

unsafe fn euc_kr2mic(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let c1 = src[pos];
        if is_highbit_set(c1) {
            let l = pg_encoding_verifymbchar(PG_EUC_KR, &src[pos..]);
            if l != 2 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_KR, &src[pos..]));
            }
            unsafe {
                out.push(LC_KS5601);
                out.push(c1);
                out.push(src[pos + 1]);
            }
            pos += 2;
        } else {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_KR, &src[pos..]));
            }
            unsafe { out.push(c1) };
            pos += 1;
        }
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

unsafe fn mic2euc_kr(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
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
        if c1 == LC_KS5601 {
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
                PG_EUC_KR,
                &src[pos..],
            ));
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

pub fn fc_euc_kr_to_mic(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_EUC_KR,
        PG_MULE_INTERNAL,
    )?;
    let n = unsafe { euc_kr2mic(a.src(), a.dest, a.no_error)? };
    Ok(Datum::from_i32(n))
}

pub fn fc_mic_to_euc_kr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_MULE_INTERNAL,
        PG_EUC_KR,
    )?;
    let n = unsafe { mic2euc_kr(a.src(), a.dest, a.no_error)? };
    Ok(Datum::from_i32(n))
}
