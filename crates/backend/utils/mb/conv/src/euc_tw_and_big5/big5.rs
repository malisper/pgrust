#![allow(non_snake_case)]

use crate::maps::local_tables::{
    Codes, B1C4, B2C3, BIG5LEVEL1TOCNSPLANE1, BIG5LEVEL2TOCNSPLANE2, CNSPLANE1TOBIG5LEVEL1,
    CNSPLANE2TOBIG5LEVEL2,
};
use crate::{LC_CNS11643_1, LC_CNS11643_2, LC_CNS11643_3, LC_CNS11643_4};

// C BinarySearchRange: interpolates within [array[mid].code, array[mid+1].code)
// ranges; `high` is len-2 by the C call convention.
fn binary_search_range(array: &[Codes], mut high: i32, code: u16) -> u16 {
    let mut low: i32 = 0;
    let mut mid = high >> 1;
    while low <= high {
        let e = &array[mid as usize];
        if e.code <= code && array[mid as usize + 1].code > code {
            if e.peer == 0 {
                return 0;
            }
            if code >= 0xa140 {
                let tmp = (((code & 0xff00) as i32) - ((e.code & 0xff00) as i32)) >> 8;
                let high_b = (code & 0x00ff) as i32;
                let low_b = (e.code & 0x00ff) as i32;
                // big5 low byte spans 0x40-0x7e and 0xa1-0xfe (radix 0x9d);
                // bias 0x22 bridges the gap between the two regions.
                let distance = tmp * 0x9d + high_b - low_b
                    + if high_b >= 0xa1 {
                        if low_b >= 0xa1 {
                            0
                        } else {
                            -0x22
                        }
                    } else if low_b >= 0xa1 {
                        0x22
                    } else {
                        0
                    };
                let tmp = ((e.peer & 0x00ff) as i32) + distance - 0x21;
                let tmp = ((e.peer & 0xff00) as i32) + ((tmp / 0x5e) << 8) + 0x21 + tmp % 0x5e;
                return tmp as u16;
            } else {
                let tmp = (((code & 0xff00) as i32) - ((e.code & 0xff00) as i32)) >> 8;
                let distance = tmp * 0x5e + ((code & 0x00ff) as i32) - ((e.code & 0x00ff) as i32);
                let low_b = (e.peer & 0x00ff) as i32;
                let tmp = low_b + distance - if low_b >= 0xa1 { 0x62 } else { 0x40 };
                let low_b = tmp % 0x9d;
                let tmp = ((e.peer & 0xff00) as i32)
                    + ((tmp / 0x9d) << 8)
                    + if low_b > 0x3e { 0x62 } else { 0x40 }
                    + low_b;
                return tmp as u16;
            }
        } else if e.code > code {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
        mid = (low + high) >> 1;
    }
    0
}

/// C `BIG5toCNS`: returns (cns, lc); lc == 0 means no mapping (cns is C's
/// `'?'` filler in that arm, unused by callers).
pub fn BIG5toCNS(big5: u16) -> (u16, u8) {
    let cns: u16;
    let mut lc: u8 = 0;
    if big5 < 0xc940 {
        for e in B1C4.iter() {
            if e[0] == big5 {
                return (e[1] | 0x8080, LC_CNS11643_4);
            }
        }
        cns = binary_search_range(&BIG5LEVEL1TOCNSPLANE1, 23, big5);
        if cns > 0 {
            lc = LC_CNS11643_1;
        }
    } else if big5 == 0xc94a {
        lc = LC_CNS11643_1;
        cns = 0x4442;
    } else {
        for e in B2C3.iter() {
            if e[0] == big5 {
                return (e[1] | 0x8080, LC_CNS11643_3);
            }
        }
        cns = binary_search_range(&BIG5LEVEL2TOCNSPLANE2, 46, big5);
        if cns > 0 {
            lc = LC_CNS11643_2;
        }
    }
    if cns == 0 {
        return (b'?' as u16, 0);
    }
    (cns | 0x8080, lc)
}

/// C `CNStoBIG5`: 0 means no mapping.
pub fn CNStoBIG5(cns: u16, lc: u8) -> u16 {
    let cns = cns & 0x7f7f;
    match lc {
        _ if lc == LC_CNS11643_1 => binary_search_range(&CNSPLANE1TOBIG5LEVEL1, 24, cns),
        _ if lc == LC_CNS11643_2 => binary_search_range(&CNSPLANE2TOBIG5LEVEL2, 47, cns),
        _ if lc == LC_CNS11643_3 => {
            for e in B2C3.iter() {
                if e[1] == cns {
                    return e[0];
                }
            }
            0
        }
        _ if lc == LC_CNS11643_4 => {
            for e in B1C4.iter() {
                if e[1] == cns {
                    return e[0];
                }
            }
            0
        }
        _ => 0,
    }
}
