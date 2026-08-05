use crate::{is_highbit_set, ConvArgs, Dst, SS2, SS3};
use datum::Datum;
use mbutils::{check_encoding_conversion_args, report_invalid_encoding};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use wchar::{pg_encoding_verifymbchar, PG_EUC_JIS_2004, PG_SHIFT_JIS_2004};

unsafe fn euc_jis_2004_2_shift_jis_2004(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    'outer: while pos < src.len() {
        let c1 = src[pos];
        if !is_highbit_set(c1) {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..]));
            }
            unsafe { out.push(c1) };
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_EUC_JIS_2004, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..]));
        }
        if c1 == SS2 && l == 2 {
            unsafe { out.push(src[pos + 1]) };
        } else if c1 == SS3 && l == 3 {
            let ku = src[pos + 1] as i32 - 0xa0;
            let ten = src[pos + 2] as i32 - 0xa0;
            match ku {
                1 | 3 | 4 | 5 | 8 | 12 | 13 | 14 | 15 => unsafe {
                    out.push((((ku + 0x1df) >> 1) - (ku >> 3) * 3) as u8)
                },
                _ => {
                    if (78..=94).contains(&ku) {
                        unsafe { out.push(((ku + 0x19b) >> 1) as u8) };
                    } else if !no_error {
                        return Err(report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..]));
                    }
                    // C's noError arm here breaks only the switch: the ku
                    // byte is skipped but ten processing still runs.
                }
            }
            if ku % 2 != 0 {
                if (1..=63).contains(&ten) {
                    unsafe { out.push((ten + 0x3f) as u8) };
                } else if (64..=94).contains(&ten) {
                    unsafe { out.push((ten + 0x40) as u8) };
                } else {
                    if no_error {
                        break 'outer;
                    }
                    return Err(report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..]));
                }
            } else {
                unsafe { out.push((ten + 0x9e) as u8) };
            }
        } else if l == 2 {
            let ku = c1 as i32 - 0xa0;
            let ten = src[pos + 1] as i32 - 0xa0;
            if (1..=62).contains(&ku) {
                unsafe { out.push(((ku + 0x101) >> 1) as u8) };
            } else if (63..=94).contains(&ku) {
                unsafe { out.push(((ku + 0x181) >> 1) as u8) };
            } else {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..]));
            }
            if ku % 2 != 0 {
                if (1..=63).contains(&ten) {
                    unsafe { out.push((ten + 0x3f) as u8) };
                } else if (64..=94).contains(&ten) {
                    unsafe { out.push((ten + 0x40) as u8) };
                } else {
                    if no_error {
                        break;
                    }
                    return Err(report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..]));
                }
            } else {
                unsafe { out.push((ten + 0x9e) as u8) };
            }
        } else {
            if no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..]));
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

// C get_ten: kubun 1 = odd ku, 0 = even ku; -1 = invalid second byte.
fn get_ten(b: i32) -> (i32, i32) {
    if (0x40..=0x7e).contains(&b) {
        (b - 0x3f, 1)
    } else if (0x80..=0x9e).contains(&b) {
        (b - 0x40, 1)
    } else if (0x9f..=0xfc).contains(&b) {
        (b - 0x9e, 0)
    } else {
        (-1, 0)
    }
}

unsafe fn shift_jis_2004_2_euc_jis_2004(src: &[u8], dest: *mut u8, no_error: bool) -> PgResult<i32> {
    let mut out = Dst(dest);
    let mut pos = 0usize;
    while pos < src.len() {
        let c1 = src[pos] as i32;
        if !is_highbit_set(src[pos]) {
            if c1 == 0 {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..]));
            }
            unsafe { out.push(c1 as u8) };
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_SHIFT_JIS_2004, &src[pos..]);
        if l < 0 || l as usize > src.len() - pos {
            if no_error {
                break;
            }
            return Err(report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..]));
        }
        if (0xa1..=0xdf).contains(&c1) && l == 1 {
            unsafe {
                out.push(SS2);
                out.push(c1 as u8);
            }
        } else if l == 2 {
            let c2 = src[pos + 1] as i32;
            let mut plane = 1;
            let ku;
            let ten;
            if (0x81..=0x9f).contains(&c1) {
                let (t, kubun) = get_ten(c2);
                ten = t;
                if ten < 0 {
                    if no_error {
                        break;
                    }
                    return Err(report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..]));
                }
                ku = (c1 << 1) - 0x100 - kubun;
            } else if (0xe0..=0xef).contains(&c1) {
                let (t, kubun) = get_ten(c2);
                ten = t;
                if ten < 0 {
                    if no_error {
                        break;
                    }
                    return Err(report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..]));
                }
                ku = (c1 << 1) - 0x180 - kubun;
            } else if (0xf0..=0xf3).contains(&c1) {
                plane = 2;
                let (t, kubun) = get_ten(c2);
                ten = t;
                if ten < 0 {
                    if no_error {
                        break;
                    }
                    return Err(report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..]));
                }
                ku = match c1 {
                    0xf0 => {
                        if kubun == 0 {
                            8
                        } else {
                            1
                        }
                    }
                    0xf1 => {
                        if kubun == 0 {
                            4
                        } else {
                            3
                        }
                    }
                    0xf2 => {
                        if kubun == 0 {
                            12
                        } else {
                            5
                        }
                    }
                    _ => {
                        if kubun == 0 {
                            14
                        } else {
                            13
                        }
                    }
                };
            } else if (0xf4..=0xfc).contains(&c1) {
                plane = 2;
                let (t, kubun) = get_ten(c2);
                ten = t;
                if ten < 0 {
                    if no_error {
                        break;
                    }
                    return Err(report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..]));
                }
                ku = if c1 == 0xf4 && kubun == 1 {
                    15
                } else {
                    (c1 << 1) - 0x19a - kubun
                };
            } else {
                if no_error {
                    break;
                }
                return Err(report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..]));
            }
            unsafe {
                if plane == 2 {
                    out.push(SS3);
                }
                out.push((ku + 0xa0) as u8);
                out.push((ten + 0xa0) as u8);
            }
        }
        pos += l as usize;
    }
    unsafe { *out.0 = 0 };
    Ok(pos as i32)
}

pub fn fc_euc_jis_2004_to_shift_jis_2004(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_EUC_JIS_2004,
        PG_SHIFT_JIS_2004,
    )?;
    let n = unsafe { euc_jis_2004_2_shift_jis_2004(a.src(), a.dest, a.no_error)? };
    Ok(Datum::from_i32(n))
}

pub fn fc_shift_jis_2004_to_euc_jis_2004(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_SHIFT_JIS_2004,
        PG_EUC_JIS_2004,
    )?;
    let n = unsafe { shift_jis_2004_2_euc_jis_2004(a.src(), a.dest, a.no_error)? };
    Ok(Datum::from_i32(n))
}
