use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_1: [symbol; 1] = [0xe1 as ::core::ffi::c_int as symbol];
static mut s_0_2: [symbol; 1] = [0xe4 as ::core::ffi::c_int as symbol];
static mut s_0_3: [symbol; 1] = [0xe9 as ::core::ffi::c_int as symbol];
static mut s_0_4: [symbol; 1] = [0xeb as ::core::ffi::c_int as symbol];
static mut s_0_5: [symbol; 1] = [0xed as ::core::ffi::c_int as symbol];
static mut s_0_6: [symbol; 1] = [0xef as ::core::ffi::c_int as symbol];
static mut s_0_7: [symbol; 1] = [0xf3 as ::core::ffi::c_int as symbol];
static mut s_0_8: [symbol; 1] = [0xf6 as ::core::ffi::c_int as symbol];
static mut s_0_9: [symbol; 1] = [0xfa as ::core::ffi::c_int as symbol];
static mut s_0_10: [symbol; 1] = [0xfc as ::core::ffi::c_int as symbol];
static mut a_0: [among; 11] = unsafe {
    [
        among {
            s_size: 0 as ::core::ffi::c_int,
            s: ::core::ptr::null::<symbol>(),
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_1: [symbol; 1] = ['I' as i32 as symbol];
static mut s_1_2: [symbol; 1] = ['Y' as i32 as symbol];
static mut a_1: [among; 3] = unsafe {
    [
        among {
            s_size: 0 as ::core::ffi::c_int,
            s: ::core::ptr::null::<symbol>(),
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 2] = ['d' as i32 as symbol, 'd' as i32 as symbol];
static mut s_2_1: [symbol; 2] = ['k' as i32 as symbol, 'k' as i32 as symbol];
static mut s_2_2: [symbol; 2] = ['t' as i32 as symbol, 't' as i32 as symbol];
static mut a_2: [among; 3] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_1: [symbol; 2] = ['s' as i32 as symbol, 'e' as i32 as symbol];
static mut s_3_2: [symbol; 2] = ['e' as i32 as symbol, 'n' as i32 as symbol];
static mut s_3_3: [symbol; 5] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_4: [symbol; 1] = ['s' as i32 as symbol];
static mut a_3: [among; 5] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_4_1: [symbol; 2] = ['i' as i32 as symbol, 'g' as i32 as symbol];
static mut s_4_2: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_4_3: [symbol; 4] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'j' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_4_4: [symbol; 4] = [
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_4_5: [symbol; 3] = [
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut a_4: [among; 6] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = ['a' as i32 as symbol, 'a' as i32 as symbol];
static mut s_5_1: [symbol; 2] = ['e' as i32 as symbol, 'e' as i32 as symbol];
static mut s_5_2: [symbol; 2] = ['o' as i32 as symbol, 'o' as i32 as symbol];
static mut s_5_3: [symbol; 2] = ['u' as i32 as symbol, 'u' as i32 as symbol];
static mut a_5: [among; 4] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 17] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    128 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_v_I: [::core::ffi::c_uchar; 20] = [
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    128 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_v_j: [::core::ffi::c_uchar; 17] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    67 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    128 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_1: [symbol; 1] = ['e' as i32 as symbol];
static mut s_2: [symbol; 1] = ['i' as i32 as symbol];
static mut s_3: [symbol; 1] = ['o' as i32 as symbol];
static mut s_4: [symbol; 1] = ['u' as i32 as symbol];
static mut s_5: [symbol; 1] = ['Y' as i32 as symbol];
static mut s_6: [symbol; 1] = ['I' as i32 as symbol];
static mut s_7: [symbol; 1] = ['Y' as i32 as symbol];
static mut s_8: [symbol; 1] = ['y' as i32 as symbol];
static mut s_9: [symbol; 1] = ['i' as i32 as symbol];
static mut s_10: [symbol; 3] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_11: [symbol; 4] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_12: [symbol; 4] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_13: [symbol; 2] = ['e' as i32 as symbol, 'n' as i32 as symbol];
static mut s_14: [symbol; 2] = ['i' as i32 as symbol, 'g' as i32 as symbol];
unsafe fn r_prelude(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    let mut c_test1: ::core::ffi::c_int = (*z).c;
    loop {
        let mut c2: ::core::ffi::c_int = (*z).c;
        (*z).bra = (*z).c;
        if (*z).c >= (*z).l
            || *(*z).p.offset(((*z).c + 0 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                != 7 as ::core::ffi::c_int
            || 340306450 as ::core::ffi::c_int
                >> (*(*z).p.offset(((*z).c + 0 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                & 1 as ::core::ffi::c_int == 0
        {
            among_var = 6 as ::core::ffi::c_int;
        } else {
            among_var = find_among(
                z,
                &raw const a_0 as *const among,
                11 as ::core::ffi::c_int,
            );
        }
        (*z).ket = (*z).c;
        match among_var {
            1 => {
                let mut ret: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_0 as *const symbol,
                );
                if ret < 0 as ::core::ffi::c_int {
                    return ret;
                }
            }
            2 => {
                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_1 as *const symbol,
                );
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
            }
            3 => {
                let mut ret_1: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_2 as *const symbol,
                );
                if ret_1 < 0 as ::core::ffi::c_int {
                    return ret_1;
                }
            }
            4 => {
                let mut ret_2: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_3 as *const symbol,
                );
                if ret_2 < 0 as ::core::ffi::c_int {
                    return ret_2;
                }
            }
            5 => {
                let mut ret_3: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_4 as *const symbol,
                );
                if ret_3 < 0 as ::core::ffi::c_int {
                    return ret_3;
                }
            }
            6 => {
                if (*z).c >= (*z).l {
                    (*z).c = c2;
                    break;
                } else {
                    (*z).c += 1;
                }
            }
            _ => {}
        }
    }
    (*z).c = c_test1;
    let mut c3: ::core::ffi::c_int = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c == (*z).l
        || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int != 'y' as i32
    {
        (*z).c = c3;
    } else {
        (*z).c += 1;
        (*z).ket = (*z).c;
        let mut ret_4: ::core::ffi::c_int = slice_from_s(
            z,
            1 as ::core::ffi::c_int,
            &raw const s_5 as *const symbol,
        );
        if ret_4 < 0 as ::core::ffi::c_int {
            return ret_4;
        }
    }
    's_196: loop {
        let mut c4: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c5: ::core::ffi::c_int = (*z).c;
            if !(in_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                232 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0)
            {
                (*z).bra = (*z).c;
                let mut c6: ::core::ffi::c_int = (*z).c;
                if (*z).c == (*z).l
                    || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                        != 'i' as i32
                {
                    current_block = 10867821094822170197;
                } else {
                    (*z).c += 1;
                    (*z).ket = (*z).c;
                    if in_grouping(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        232 as ::core::ffi::c_int,
                        0 as ::core::ffi::c_int,
                    ) != 0
                    {
                        current_block = 10867821094822170197;
                    } else {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            1 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 11959868629104233112;
                    }
                }
                match current_block {
                    10867821094822170197 => {
                        (*z).c = c6;
                        if (*z).c == (*z).l
                            || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                                != 'y' as i32
                        {
                            current_block = 2806015471427676362;
                        } else {
                            (*z).c += 1;
                            (*z).ket = (*z).c;
                            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                                z,
                                1 as ::core::ffi::c_int,
                                &raw const s_7 as *const symbol,
                            );
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 11959868629104233112;
                        }
                    }
                    _ => {}
                }
                match current_block {
                    2806015471427676362 => {}
                    _ => {
                        (*z).c = c5;
                        continue 's_196;
                    }
                }
            }
            (*z).c = c5;
            if (*z).c >= (*z).l {
                break;
            }
            (*z).c += 1;
        }
        (*z).c = c4;
        break;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c_test1: ::core::ffi::c_int = (*z).c;
    (*z).c = (*z).c + 3 as ::core::ffi::c_int;
    if (*z).c > (*z).l {
        return 0 as ::core::ffi::c_int;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
    (*z).c = c_test1;
    let mut ret: ::core::ffi::c_int = out_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        232 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if ret < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c += ret;
    let mut ret_0: ::core::ffi::c_int = in_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        232 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c += ret_0;
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = (*z).c;
    if !(*(*z).I.offset(2 as ::core::ffi::c_int as isize)
        >= *(*z).I.offset(0 as ::core::ffi::c_int as isize))
    {
        *(*z).I.offset(2 as ::core::ffi::c_int as isize) = *(*z)
            .I
            .offset(0 as ::core::ffi::c_int as isize);
    }
    let mut ret_1: ::core::ffi::c_int = out_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        232 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if ret_1 < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c += ret_1;
    let mut ret_2: ::core::ffi::c_int = in_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        232 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if ret_2 < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c += ret_2;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_postlude(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    loop {
        let mut c1: ::core::ffi::c_int = (*z).c;
        (*z).bra = (*z).c;
        if (*z).c >= (*z).l
            || *(*z).p.offset(((*z).c + 0 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 73 as ::core::ffi::c_int
                && *(*z).p.offset(((*z).c + 0 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 89 as ::core::ffi::c_int
        {
            among_var = 3 as ::core::ffi::c_int;
        } else {
            among_var = find_among(
                z,
                &raw const a_1 as *const among,
                3 as ::core::ffi::c_int,
            );
        }
        (*z).ket = (*z).c;
        match among_var {
            1 => {
                let mut ret: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_8 as *const symbol,
                );
                if ret < 0 as ::core::ffi::c_int {
                    return ret;
                }
            }
            2 => {
                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_9 as *const symbol,
                );
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
            }
            3 => {
                if (*z).c >= (*z).l {
                    (*z).c = c1;
                    break;
                } else {
                    (*z).c += 1;
                }
            }
            _ => {}
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_R1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(2 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_R2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(1 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_undouble(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m_test1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 1050640 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_2 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c = (*z).l - m_test1;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= 1;
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_e_ending(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(3 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'e' as i32
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= 1;
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut m_test1: ::core::ffi::c_int = (*z).l - (*z).c;
    if out_grouping_b(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        232 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c = (*z).l - m_test1;
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    *(*z).I.offset(3 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    let mut ret_1: ::core::ffi::c_int = r_undouble(z);
    if ret_1 <= 0 as ::core::ffi::c_int {
        return ret_1;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_en_ending(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if out_grouping_b(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        232 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c = (*z).l - m1;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_10 as *const symbol) == 0 {
        (*z).c = (*z).l - m2;
    } else {
        return 0 as ::core::ffi::c_int
    }
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    let mut ret_1: ::core::ffi::c_int = r_undouble(z);
    if ret_1 <= 0 as ::core::ffi::c_int {
        return ret_1;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_standard_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !((*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 540704 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0)
    {
        among_var = find_among_b(
            z,
            &raw const a_3 as *const among,
            5 as ::core::ffi::c_int,
        );
        if !(among_var == 0) {
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    current_block = 11875828834189669668;
                    match current_block {
                        11875828834189669668 => {
                            let mut ret: ::core::ffi::c_int = r_R1(z);
                            if !(ret == 0 as ::core::ffi::c_int) {
                                if ret < 0 as ::core::ffi::c_int {
                                    return ret;
                                }
                                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    4 as ::core::ffi::c_int,
                                    &raw const s_11 as *const symbol,
                                );
                                if ret_0 < 0 as ::core::ffi::c_int {
                                    return ret_0;
                                }
                            }
                        }
                        2979737022853876585 => {
                            let mut ret_1: ::core::ffi::c_int = r_en_ending(z);
                            if !(ret_1 == 0 as ::core::ffi::c_int) {
                                if ret_1 < 0 as ::core::ffi::c_int {
                                    return ret_1;
                                }
                            }
                        }
                        _ => {
                            let mut ret_2: ::core::ffi::c_int = r_R1(z);
                            if !(ret_2 == 0 as ::core::ffi::c_int) {
                                if ret_2 < 0 as ::core::ffi::c_int {
                                    return ret_2;
                                }
                                if !(out_grouping_b(
                                    z,
                                    &raw const g_v_j as *const ::core::ffi::c_uchar,
                                    97 as ::core::ffi::c_int,
                                    232 as ::core::ffi::c_int,
                                    0 as ::core::ffi::c_int,
                                ) != 0)
                                {
                                    let mut ret_3: ::core::ffi::c_int = slice_del(z);
                                    if ret_3 < 0 as ::core::ffi::c_int {
                                        return ret_3;
                                    }
                                }
                            }
                        }
                    }
                }
                2 => {
                    current_block = 2979737022853876585;
                    match current_block {
                        11875828834189669668 => {
                            let mut ret: ::core::ffi::c_int = r_R1(z);
                            if !(ret == 0 as ::core::ffi::c_int) {
                                if ret < 0 as ::core::ffi::c_int {
                                    return ret;
                                }
                                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    4 as ::core::ffi::c_int,
                                    &raw const s_11 as *const symbol,
                                );
                                if ret_0 < 0 as ::core::ffi::c_int {
                                    return ret_0;
                                }
                            }
                        }
                        2979737022853876585 => {
                            let mut ret_1: ::core::ffi::c_int = r_en_ending(z);
                            if !(ret_1 == 0 as ::core::ffi::c_int) {
                                if ret_1 < 0 as ::core::ffi::c_int {
                                    return ret_1;
                                }
                            }
                        }
                        _ => {
                            let mut ret_2: ::core::ffi::c_int = r_R1(z);
                            if !(ret_2 == 0 as ::core::ffi::c_int) {
                                if ret_2 < 0 as ::core::ffi::c_int {
                                    return ret_2;
                                }
                                if !(out_grouping_b(
                                    z,
                                    &raw const g_v_j as *const ::core::ffi::c_uchar,
                                    97 as ::core::ffi::c_int,
                                    232 as ::core::ffi::c_int,
                                    0 as ::core::ffi::c_int,
                                ) != 0)
                                {
                                    let mut ret_3: ::core::ffi::c_int = slice_del(z);
                                    if ret_3 < 0 as ::core::ffi::c_int {
                                        return ret_3;
                                    }
                                }
                            }
                        }
                    }
                }
                3 => {
                    current_block = 15976848397966268834;
                    match current_block {
                        11875828834189669668 => {
                            let mut ret: ::core::ffi::c_int = r_R1(z);
                            if !(ret == 0 as ::core::ffi::c_int) {
                                if ret < 0 as ::core::ffi::c_int {
                                    return ret;
                                }
                                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    4 as ::core::ffi::c_int,
                                    &raw const s_11 as *const symbol,
                                );
                                if ret_0 < 0 as ::core::ffi::c_int {
                                    return ret_0;
                                }
                            }
                        }
                        2979737022853876585 => {
                            let mut ret_1: ::core::ffi::c_int = r_en_ending(z);
                            if !(ret_1 == 0 as ::core::ffi::c_int) {
                                if ret_1 < 0 as ::core::ffi::c_int {
                                    return ret_1;
                                }
                            }
                        }
                        _ => {
                            let mut ret_2: ::core::ffi::c_int = r_R1(z);
                            if !(ret_2 == 0 as ::core::ffi::c_int) {
                                if ret_2 < 0 as ::core::ffi::c_int {
                                    return ret_2;
                                }
                                if !(out_grouping_b(
                                    z,
                                    &raw const g_v_j as *const ::core::ffi::c_uchar,
                                    97 as ::core::ffi::c_int,
                                    232 as ::core::ffi::c_int,
                                    0 as ::core::ffi::c_int,
                                ) != 0)
                                {
                                    let mut ret_3: ::core::ffi::c_int = slice_del(z);
                                    if ret_3 < 0 as ::core::ffi::c_int {
                                        return ret_3;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    (*z).c = (*z).l - m1;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_4: ::core::ffi::c_int = r_e_ending(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !(eq_s_b(z, 4 as ::core::ffi::c_int, &raw const s_12 as *const symbol) == 0) {
        (*z).bra = (*z).c;
        let mut ret_5: ::core::ffi::c_int = r_R2(z);
        if !(ret_5 == 0 as ::core::ffi::c_int) {
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'c' as i32
            {
                (*z).c = (*z).l - m4;
                let mut ret_6: ::core::ffi::c_int = slice_del(z);
                if ret_6 < 0 as ::core::ffi::c_int {
                    return ret_6;
                }
                (*z).ket = (*z).c;
                if !(eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_13 as *const symbol)
                    == 0)
                {
                    (*z).bra = (*z).c;
                    let mut ret_7: ::core::ffi::c_int = r_en_ending(z);
                    if !(ret_7 == 0 as ::core::ffi::c_int) {
                        if ret_7 < 0 as ::core::ffi::c_int {
                            return ret_7;
                        }
                    }
                }
            } else {
                (*z).c -= 1;
            }
        }
    }
    (*z).c = (*z).l - m3;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !((*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 264336 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0)
    {
        among_var = find_among_b(
            z,
            &raw const a_4 as *const among,
            6 as ::core::ffi::c_int,
        );
        if !(among_var == 0) {
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    current_block = 9241535491006583629;
                    match current_block {
                        9241535491006583629 => {
                            let mut ret_8: ::core::ffi::c_int = r_R2(z);
                            if !(ret_8 == 0 as ::core::ffi::c_int) {
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                                let mut ret_9: ::core::ffi::c_int = slice_del(z);
                                if ret_9 < 0 as ::core::ffi::c_int {
                                    return ret_9;
                                }
                                let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                                (*z).ket = (*z).c;
                                if eq_s_b(
                                    z,
                                    2 as ::core::ffi::c_int,
                                    &raw const s_14 as *const symbol,
                                ) == 0
                                {
                                    current_block = 6189995603851630255;
                                } else {
                                    (*z).bra = (*z).c;
                                    let mut ret_10: ::core::ffi::c_int = r_R2(z);
                                    if ret_10 == 0 as ::core::ffi::c_int {
                                        current_block = 6189995603851630255;
                                    } else {
                                        if ret_10 < 0 as ::core::ffi::c_int {
                                            return ret_10;
                                        }
                                        let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                                        if (*z).c <= (*z).lb
                                            || *(*z)
                                                .p
                                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                                as ::core::ffi::c_int != 'e' as i32
                                        {
                                            (*z).c = (*z).l - m7;
                                            let mut ret_11: ::core::ffi::c_int = slice_del(z);
                                            if ret_11 < 0 as ::core::ffi::c_int {
                                                return ret_11;
                                            }
                                            current_block = 10764037593745839472;
                                        } else {
                                            (*z).c -= 1;
                                            current_block = 6189995603851630255;
                                        }
                                    }
                                }
                                match current_block {
                                    10764037593745839472 => {}
                                    _ => {
                                        (*z).c = (*z).l - m6;
                                        let mut ret_12: ::core::ffi::c_int = r_undouble(z);
                                        if !(ret_12 == 0 as ::core::ffi::c_int) {
                                            if ret_12 < 0 as ::core::ffi::c_int {
                                                return ret_12;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        14851765859726653900 => {
                            let mut ret_18: ::core::ffi::c_int = r_R2(z);
                            if !(ret_18 == 0 as ::core::ffi::c_int) {
                                if ret_18 < 0 as ::core::ffi::c_int {
                                    return ret_18;
                                }
                                let mut ret_19: ::core::ffi::c_int = slice_del(z);
                                if ret_19 < 0 as ::core::ffi::c_int {
                                    return ret_19;
                                }
                            }
                        }
                        14141370668937312244 => {
                            let mut ret_15: ::core::ffi::c_int = r_R2(z);
                            if !(ret_15 == 0 as ::core::ffi::c_int) {
                                if ret_15 < 0 as ::core::ffi::c_int {
                                    return ret_15;
                                }
                                let mut ret_16: ::core::ffi::c_int = slice_del(z);
                                if ret_16 < 0 as ::core::ffi::c_int {
                                    return ret_16;
                                }
                                let mut ret_17: ::core::ffi::c_int = r_e_ending(z);
                                if !(ret_17 == 0 as ::core::ffi::c_int) {
                                    if ret_17 < 0 as ::core::ffi::c_int {
                                        return ret_17;
                                    }
                                }
                            }
                        }
                        6838274324784804404 => {
                            let mut ret_13: ::core::ffi::c_int = r_R2(z);
                            if !(ret_13 == 0 as ::core::ffi::c_int) {
                                if ret_13 < 0 as ::core::ffi::c_int {
                                    return ret_13;
                                }
                                let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                                if (*z).c <= (*z).lb
                                    || *(*z)
                                        .p
                                        .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                        as ::core::ffi::c_int != 'e' as i32
                                {
                                    (*z).c = (*z).l - m8;
                                    let mut ret_14: ::core::ffi::c_int = slice_del(z);
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                } else {
                                    (*z).c -= 1;
                                }
                            }
                        }
                        _ => {
                            let mut ret_20: ::core::ffi::c_int = r_R2(z);
                            if !(ret_20 == 0 as ::core::ffi::c_int) {
                                if ret_20 < 0 as ::core::ffi::c_int {
                                    return ret_20;
                                }
                                if !(*(*z).I.offset(3 as ::core::ffi::c_int as isize) == 0)
                                {
                                    let mut ret_21: ::core::ffi::c_int = slice_del(z);
                                    if ret_21 < 0 as ::core::ffi::c_int {
                                        return ret_21;
                                    }
                                }
                            }
                        }
                    }
                }
                2 => {
                    current_block = 6838274324784804404;
                    match current_block {
                        9241535491006583629 => {
                            let mut ret_8: ::core::ffi::c_int = r_R2(z);
                            if !(ret_8 == 0 as ::core::ffi::c_int) {
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                                let mut ret_9: ::core::ffi::c_int = slice_del(z);
                                if ret_9 < 0 as ::core::ffi::c_int {
                                    return ret_9;
                                }
                                let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                                (*z).ket = (*z).c;
                                if eq_s_b(
                                    z,
                                    2 as ::core::ffi::c_int,
                                    &raw const s_14 as *const symbol,
                                ) == 0
                                {
                                    current_block = 6189995603851630255;
                                } else {
                                    (*z).bra = (*z).c;
                                    let mut ret_10: ::core::ffi::c_int = r_R2(z);
                                    if ret_10 == 0 as ::core::ffi::c_int {
                                        current_block = 6189995603851630255;
                                    } else {
                                        if ret_10 < 0 as ::core::ffi::c_int {
                                            return ret_10;
                                        }
                                        let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                                        if (*z).c <= (*z).lb
                                            || *(*z)
                                                .p
                                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                                as ::core::ffi::c_int != 'e' as i32
                                        {
                                            (*z).c = (*z).l - m7;
                                            let mut ret_11: ::core::ffi::c_int = slice_del(z);
                                            if ret_11 < 0 as ::core::ffi::c_int {
                                                return ret_11;
                                            }
                                            current_block = 10764037593745839472;
                                        } else {
                                            (*z).c -= 1;
                                            current_block = 6189995603851630255;
                                        }
                                    }
                                }
                                match current_block {
                                    10764037593745839472 => {}
                                    _ => {
                                        (*z).c = (*z).l - m6;
                                        let mut ret_12: ::core::ffi::c_int = r_undouble(z);
                                        if !(ret_12 == 0 as ::core::ffi::c_int) {
                                            if ret_12 < 0 as ::core::ffi::c_int {
                                                return ret_12;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        14851765859726653900 => {
                            let mut ret_18: ::core::ffi::c_int = r_R2(z);
                            if !(ret_18 == 0 as ::core::ffi::c_int) {
                                if ret_18 < 0 as ::core::ffi::c_int {
                                    return ret_18;
                                }
                                let mut ret_19: ::core::ffi::c_int = slice_del(z);
                                if ret_19 < 0 as ::core::ffi::c_int {
                                    return ret_19;
                                }
                            }
                        }
                        14141370668937312244 => {
                            let mut ret_15: ::core::ffi::c_int = r_R2(z);
                            if !(ret_15 == 0 as ::core::ffi::c_int) {
                                if ret_15 < 0 as ::core::ffi::c_int {
                                    return ret_15;
                                }
                                let mut ret_16: ::core::ffi::c_int = slice_del(z);
                                if ret_16 < 0 as ::core::ffi::c_int {
                                    return ret_16;
                                }
                                let mut ret_17: ::core::ffi::c_int = r_e_ending(z);
                                if !(ret_17 == 0 as ::core::ffi::c_int) {
                                    if ret_17 < 0 as ::core::ffi::c_int {
                                        return ret_17;
                                    }
                                }
                            }
                        }
                        6838274324784804404 => {
                            let mut ret_13: ::core::ffi::c_int = r_R2(z);
                            if !(ret_13 == 0 as ::core::ffi::c_int) {
                                if ret_13 < 0 as ::core::ffi::c_int {
                                    return ret_13;
                                }
                                let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                                if (*z).c <= (*z).lb
                                    || *(*z)
                                        .p
                                        .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                        as ::core::ffi::c_int != 'e' as i32
                                {
                                    (*z).c = (*z).l - m8;
                                    let mut ret_14: ::core::ffi::c_int = slice_del(z);
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                } else {
                                    (*z).c -= 1;
                                }
                            }
                        }
                        _ => {
                            let mut ret_20: ::core::ffi::c_int = r_R2(z);
                            if !(ret_20 == 0 as ::core::ffi::c_int) {
                                if ret_20 < 0 as ::core::ffi::c_int {
                                    return ret_20;
                                }
                                if !(*(*z).I.offset(3 as ::core::ffi::c_int as isize) == 0)
                                {
                                    let mut ret_21: ::core::ffi::c_int = slice_del(z);
                                    if ret_21 < 0 as ::core::ffi::c_int {
                                        return ret_21;
                                    }
                                }
                            }
                        }
                    }
                }
                3 => {
                    current_block = 14141370668937312244;
                    match current_block {
                        9241535491006583629 => {
                            let mut ret_8: ::core::ffi::c_int = r_R2(z);
                            if !(ret_8 == 0 as ::core::ffi::c_int) {
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                                let mut ret_9: ::core::ffi::c_int = slice_del(z);
                                if ret_9 < 0 as ::core::ffi::c_int {
                                    return ret_9;
                                }
                                let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                                (*z).ket = (*z).c;
                                if eq_s_b(
                                    z,
                                    2 as ::core::ffi::c_int,
                                    &raw const s_14 as *const symbol,
                                ) == 0
                                {
                                    current_block = 6189995603851630255;
                                } else {
                                    (*z).bra = (*z).c;
                                    let mut ret_10: ::core::ffi::c_int = r_R2(z);
                                    if ret_10 == 0 as ::core::ffi::c_int {
                                        current_block = 6189995603851630255;
                                    } else {
                                        if ret_10 < 0 as ::core::ffi::c_int {
                                            return ret_10;
                                        }
                                        let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                                        if (*z).c <= (*z).lb
                                            || *(*z)
                                                .p
                                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                                as ::core::ffi::c_int != 'e' as i32
                                        {
                                            (*z).c = (*z).l - m7;
                                            let mut ret_11: ::core::ffi::c_int = slice_del(z);
                                            if ret_11 < 0 as ::core::ffi::c_int {
                                                return ret_11;
                                            }
                                            current_block = 10764037593745839472;
                                        } else {
                                            (*z).c -= 1;
                                            current_block = 6189995603851630255;
                                        }
                                    }
                                }
                                match current_block {
                                    10764037593745839472 => {}
                                    _ => {
                                        (*z).c = (*z).l - m6;
                                        let mut ret_12: ::core::ffi::c_int = r_undouble(z);
                                        if !(ret_12 == 0 as ::core::ffi::c_int) {
                                            if ret_12 < 0 as ::core::ffi::c_int {
                                                return ret_12;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        14851765859726653900 => {
                            let mut ret_18: ::core::ffi::c_int = r_R2(z);
                            if !(ret_18 == 0 as ::core::ffi::c_int) {
                                if ret_18 < 0 as ::core::ffi::c_int {
                                    return ret_18;
                                }
                                let mut ret_19: ::core::ffi::c_int = slice_del(z);
                                if ret_19 < 0 as ::core::ffi::c_int {
                                    return ret_19;
                                }
                            }
                        }
                        14141370668937312244 => {
                            let mut ret_15: ::core::ffi::c_int = r_R2(z);
                            if !(ret_15 == 0 as ::core::ffi::c_int) {
                                if ret_15 < 0 as ::core::ffi::c_int {
                                    return ret_15;
                                }
                                let mut ret_16: ::core::ffi::c_int = slice_del(z);
                                if ret_16 < 0 as ::core::ffi::c_int {
                                    return ret_16;
                                }
                                let mut ret_17: ::core::ffi::c_int = r_e_ending(z);
                                if !(ret_17 == 0 as ::core::ffi::c_int) {
                                    if ret_17 < 0 as ::core::ffi::c_int {
                                        return ret_17;
                                    }
                                }
                            }
                        }
                        6838274324784804404 => {
                            let mut ret_13: ::core::ffi::c_int = r_R2(z);
                            if !(ret_13 == 0 as ::core::ffi::c_int) {
                                if ret_13 < 0 as ::core::ffi::c_int {
                                    return ret_13;
                                }
                                let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                                if (*z).c <= (*z).lb
                                    || *(*z)
                                        .p
                                        .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                        as ::core::ffi::c_int != 'e' as i32
                                {
                                    (*z).c = (*z).l - m8;
                                    let mut ret_14: ::core::ffi::c_int = slice_del(z);
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                } else {
                                    (*z).c -= 1;
                                }
                            }
                        }
                        _ => {
                            let mut ret_20: ::core::ffi::c_int = r_R2(z);
                            if !(ret_20 == 0 as ::core::ffi::c_int) {
                                if ret_20 < 0 as ::core::ffi::c_int {
                                    return ret_20;
                                }
                                if !(*(*z).I.offset(3 as ::core::ffi::c_int as isize) == 0)
                                {
                                    let mut ret_21: ::core::ffi::c_int = slice_del(z);
                                    if ret_21 < 0 as ::core::ffi::c_int {
                                        return ret_21;
                                    }
                                }
                            }
                        }
                    }
                }
                4 => {
                    current_block = 14851765859726653900;
                    match current_block {
                        9241535491006583629 => {
                            let mut ret_8: ::core::ffi::c_int = r_R2(z);
                            if !(ret_8 == 0 as ::core::ffi::c_int) {
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                                let mut ret_9: ::core::ffi::c_int = slice_del(z);
                                if ret_9 < 0 as ::core::ffi::c_int {
                                    return ret_9;
                                }
                                let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                                (*z).ket = (*z).c;
                                if eq_s_b(
                                    z,
                                    2 as ::core::ffi::c_int,
                                    &raw const s_14 as *const symbol,
                                ) == 0
                                {
                                    current_block = 6189995603851630255;
                                } else {
                                    (*z).bra = (*z).c;
                                    let mut ret_10: ::core::ffi::c_int = r_R2(z);
                                    if ret_10 == 0 as ::core::ffi::c_int {
                                        current_block = 6189995603851630255;
                                    } else {
                                        if ret_10 < 0 as ::core::ffi::c_int {
                                            return ret_10;
                                        }
                                        let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                                        if (*z).c <= (*z).lb
                                            || *(*z)
                                                .p
                                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                                as ::core::ffi::c_int != 'e' as i32
                                        {
                                            (*z).c = (*z).l - m7;
                                            let mut ret_11: ::core::ffi::c_int = slice_del(z);
                                            if ret_11 < 0 as ::core::ffi::c_int {
                                                return ret_11;
                                            }
                                            current_block = 10764037593745839472;
                                        } else {
                                            (*z).c -= 1;
                                            current_block = 6189995603851630255;
                                        }
                                    }
                                }
                                match current_block {
                                    10764037593745839472 => {}
                                    _ => {
                                        (*z).c = (*z).l - m6;
                                        let mut ret_12: ::core::ffi::c_int = r_undouble(z);
                                        if !(ret_12 == 0 as ::core::ffi::c_int) {
                                            if ret_12 < 0 as ::core::ffi::c_int {
                                                return ret_12;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        14851765859726653900 => {
                            let mut ret_18: ::core::ffi::c_int = r_R2(z);
                            if !(ret_18 == 0 as ::core::ffi::c_int) {
                                if ret_18 < 0 as ::core::ffi::c_int {
                                    return ret_18;
                                }
                                let mut ret_19: ::core::ffi::c_int = slice_del(z);
                                if ret_19 < 0 as ::core::ffi::c_int {
                                    return ret_19;
                                }
                            }
                        }
                        14141370668937312244 => {
                            let mut ret_15: ::core::ffi::c_int = r_R2(z);
                            if !(ret_15 == 0 as ::core::ffi::c_int) {
                                if ret_15 < 0 as ::core::ffi::c_int {
                                    return ret_15;
                                }
                                let mut ret_16: ::core::ffi::c_int = slice_del(z);
                                if ret_16 < 0 as ::core::ffi::c_int {
                                    return ret_16;
                                }
                                let mut ret_17: ::core::ffi::c_int = r_e_ending(z);
                                if !(ret_17 == 0 as ::core::ffi::c_int) {
                                    if ret_17 < 0 as ::core::ffi::c_int {
                                        return ret_17;
                                    }
                                }
                            }
                        }
                        6838274324784804404 => {
                            let mut ret_13: ::core::ffi::c_int = r_R2(z);
                            if !(ret_13 == 0 as ::core::ffi::c_int) {
                                if ret_13 < 0 as ::core::ffi::c_int {
                                    return ret_13;
                                }
                                let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                                if (*z).c <= (*z).lb
                                    || *(*z)
                                        .p
                                        .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                        as ::core::ffi::c_int != 'e' as i32
                                {
                                    (*z).c = (*z).l - m8;
                                    let mut ret_14: ::core::ffi::c_int = slice_del(z);
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                } else {
                                    (*z).c -= 1;
                                }
                            }
                        }
                        _ => {
                            let mut ret_20: ::core::ffi::c_int = r_R2(z);
                            if !(ret_20 == 0 as ::core::ffi::c_int) {
                                if ret_20 < 0 as ::core::ffi::c_int {
                                    return ret_20;
                                }
                                if !(*(*z).I.offset(3 as ::core::ffi::c_int as isize) == 0)
                                {
                                    let mut ret_21: ::core::ffi::c_int = slice_del(z);
                                    if ret_21 < 0 as ::core::ffi::c_int {
                                        return ret_21;
                                    }
                                }
                            }
                        }
                    }
                }
                5 => {
                    current_block = 1677945370889843322;
                    match current_block {
                        9241535491006583629 => {
                            let mut ret_8: ::core::ffi::c_int = r_R2(z);
                            if !(ret_8 == 0 as ::core::ffi::c_int) {
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                                let mut ret_9: ::core::ffi::c_int = slice_del(z);
                                if ret_9 < 0 as ::core::ffi::c_int {
                                    return ret_9;
                                }
                                let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                                (*z).ket = (*z).c;
                                if eq_s_b(
                                    z,
                                    2 as ::core::ffi::c_int,
                                    &raw const s_14 as *const symbol,
                                ) == 0
                                {
                                    current_block = 6189995603851630255;
                                } else {
                                    (*z).bra = (*z).c;
                                    let mut ret_10: ::core::ffi::c_int = r_R2(z);
                                    if ret_10 == 0 as ::core::ffi::c_int {
                                        current_block = 6189995603851630255;
                                    } else {
                                        if ret_10 < 0 as ::core::ffi::c_int {
                                            return ret_10;
                                        }
                                        let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                                        if (*z).c <= (*z).lb
                                            || *(*z)
                                                .p
                                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                                as ::core::ffi::c_int != 'e' as i32
                                        {
                                            (*z).c = (*z).l - m7;
                                            let mut ret_11: ::core::ffi::c_int = slice_del(z);
                                            if ret_11 < 0 as ::core::ffi::c_int {
                                                return ret_11;
                                            }
                                            current_block = 10764037593745839472;
                                        } else {
                                            (*z).c -= 1;
                                            current_block = 6189995603851630255;
                                        }
                                    }
                                }
                                match current_block {
                                    10764037593745839472 => {}
                                    _ => {
                                        (*z).c = (*z).l - m6;
                                        let mut ret_12: ::core::ffi::c_int = r_undouble(z);
                                        if !(ret_12 == 0 as ::core::ffi::c_int) {
                                            if ret_12 < 0 as ::core::ffi::c_int {
                                                return ret_12;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        14851765859726653900 => {
                            let mut ret_18: ::core::ffi::c_int = r_R2(z);
                            if !(ret_18 == 0 as ::core::ffi::c_int) {
                                if ret_18 < 0 as ::core::ffi::c_int {
                                    return ret_18;
                                }
                                let mut ret_19: ::core::ffi::c_int = slice_del(z);
                                if ret_19 < 0 as ::core::ffi::c_int {
                                    return ret_19;
                                }
                            }
                        }
                        14141370668937312244 => {
                            let mut ret_15: ::core::ffi::c_int = r_R2(z);
                            if !(ret_15 == 0 as ::core::ffi::c_int) {
                                if ret_15 < 0 as ::core::ffi::c_int {
                                    return ret_15;
                                }
                                let mut ret_16: ::core::ffi::c_int = slice_del(z);
                                if ret_16 < 0 as ::core::ffi::c_int {
                                    return ret_16;
                                }
                                let mut ret_17: ::core::ffi::c_int = r_e_ending(z);
                                if !(ret_17 == 0 as ::core::ffi::c_int) {
                                    if ret_17 < 0 as ::core::ffi::c_int {
                                        return ret_17;
                                    }
                                }
                            }
                        }
                        6838274324784804404 => {
                            let mut ret_13: ::core::ffi::c_int = r_R2(z);
                            if !(ret_13 == 0 as ::core::ffi::c_int) {
                                if ret_13 < 0 as ::core::ffi::c_int {
                                    return ret_13;
                                }
                                let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                                if (*z).c <= (*z).lb
                                    || *(*z)
                                        .p
                                        .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                        as ::core::ffi::c_int != 'e' as i32
                                {
                                    (*z).c = (*z).l - m8;
                                    let mut ret_14: ::core::ffi::c_int = slice_del(z);
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                } else {
                                    (*z).c -= 1;
                                }
                            }
                        }
                        _ => {
                            let mut ret_20: ::core::ffi::c_int = r_R2(z);
                            if !(ret_20 == 0 as ::core::ffi::c_int) {
                                if ret_20 < 0 as ::core::ffi::c_int {
                                    return ret_20;
                                }
                                if !(*(*z).I.offset(3 as ::core::ffi::c_int as isize) == 0)
                                {
                                    let mut ret_21: ::core::ffi::c_int = slice_del(z);
                                    if ret_21 < 0 as ::core::ffi::c_int {
                                        return ret_21;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    (*z).c = (*z).l - m5;
    let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
    if !(out_grouping_b(
        z,
        &raw const g_v_I as *const ::core::ffi::c_uchar,
        73 as ::core::ffi::c_int,
        232 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0)
    {
        let mut m_test10: ::core::ffi::c_int = (*z).l - (*z).c;
        if !((*z).c - 1 as ::core::ffi::c_int <= (*z).lb
            || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                != 3 as ::core::ffi::c_int
            || 2129954 as ::core::ffi::c_int
                >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                & 1 as ::core::ffi::c_int == 0)
        {
            if !(find_among_b(z, &raw const a_5 as *const among, 4 as ::core::ffi::c_int)
                == 0)
            {
                if !(out_grouping_b(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    232 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0)
                {
                    (*z).c = (*z).l - m_test10;
                    (*z).ket = (*z).c;
                    if !((*z).c <= (*z).lb) {
                        (*z).c -= 1;
                        (*z).bra = (*z).c;
                        let mut ret_22: ::core::ffi::c_int = slice_del(z);
                        if ret_22 < 0 as ::core::ffi::c_int {
                            return ret_22;
                        }
                    }
                }
            }
        }
    }
    (*z).c = (*z).l - m9;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn dutch_ISO_8859_1_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_prelude(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = c1;
    let mut c2: ::core::ffi::c_int = (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_mark_regions(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = c2;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut ret_1: ::core::ffi::c_int = r_standard_suffix(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).lb;
    let mut c3: ::core::ffi::c_int = (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_postlude(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = c3;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn dutch_ISO_8859_1_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 4 as ::core::ffi::c_int);
}
pub unsafe fn dutch_ISO_8859_1_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
