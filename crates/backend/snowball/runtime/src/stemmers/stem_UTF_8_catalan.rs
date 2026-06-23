use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_1: [symbol; 2] = [
    0xc2 as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
];
static mut s_0_2: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_0_3: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_0_4: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_0_5: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_0_6: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_0_7: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
];
static mut s_0_8: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_0_9: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_0_10: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_0_11: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_0_12: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 13] = unsafe {
    [
        among {
            s_size: 0 as ::core::ffi::c_int,
            s: ::core::ptr::null::<symbol>(),
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_11 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_12 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 2] = ['l' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_1: [symbol; 3] = [
    '-' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_2: [symbol; 4] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_3: [symbol; 2] = ['l' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_4: [symbol; 2] = ['m' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_5: [symbol; 3] = [
    '-' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_6: [symbol; 2] = ['s' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_7: [symbol; 3] = [
    '-' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_8: [symbol; 2] = ['h' as i32 as symbol, 'i' as i32 as symbol];
static mut s_1_9: [symbol; 3] = [
    '\'' as i32 as symbol,
    'h' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_10: [symbol; 2] = ['l' as i32 as symbol, 'i' as i32 as symbol];
static mut s_1_11: [symbol; 3] = [
    '-' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_12: [symbol; 2] = ['\'' as i32 as symbol, 'l' as i32 as symbol];
static mut s_1_13: [symbol; 2] = ['\'' as i32 as symbol, 'm' as i32 as symbol];
static mut s_1_14: [symbol; 2] = ['-' as i32 as symbol, 'm' as i32 as symbol];
static mut s_1_15: [symbol; 2] = ['\'' as i32 as symbol, 'n' as i32 as symbol];
static mut s_1_16: [symbol; 2] = ['-' as i32 as symbol, 'n' as i32 as symbol];
static mut s_1_17: [symbol; 2] = ['h' as i32 as symbol, 'o' as i32 as symbol];
static mut s_1_18: [symbol; 3] = [
    '\'' as i32 as symbol,
    'h' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_19: [symbol; 2] = ['l' as i32 as symbol, 'o' as i32 as symbol];
static mut s_1_20: [symbol; 4] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_21: [symbol; 2] = ['\'' as i32 as symbol, 's' as i32 as symbol];
static mut s_1_22: [symbol; 3] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_23: [symbol; 5] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_24: [symbol; 3] = [
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_25: [symbol; 4] = [
    '-' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_26: [symbol; 3] = [
    '\'' as i32 as symbol,
    'l' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_27: [symbol; 3] = [
    '-' as i32 as symbol,
    'l' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_28: [symbol; 3] = [
    '\'' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_29: [symbol; 3] = [
    '-' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_30: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_31: [symbol; 3] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_32: [symbol; 5] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_33: [symbol; 3] = [
    'n' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_34: [symbol; 4] = [
    '-' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_35: [symbol; 3] = [
    'v' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_36: [symbol; 2] = ['u' as i32 as symbol, 's' as i32 as symbol];
static mut s_1_37: [symbol; 3] = [
    '-' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_38: [symbol; 2] = ['\'' as i32 as symbol, 't' as i32 as symbol];
static mut a_1: [among; 39] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_25 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_32 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_34 as *const symbol,
            substring_i: 33 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_37 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_1: [symbol; 7] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_2: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_3: [symbol; 3] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_4: [symbol; 5] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_5: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_6: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_7: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_8: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_9: [symbol; 4] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_10: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_11: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_12: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_13: [symbol; 7] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_14: [symbol; 4] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_15: [symbol; 4] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_16: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'o' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_17: [symbol; 3] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_18: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_19: [symbol; 9] = [
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_20: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_21: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_22: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_23: [symbol; 5] = [
    's' as i32 as symbol,
    'f' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_24: [symbol; 3] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_25: [symbol; 4] = [
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_26: [symbol; 5] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_27: [symbol; 5] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_28: [symbol; 3] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_29: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_30: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_31: [symbol; 4] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_32: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_33: [symbol; 3] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_34: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_35: [symbol; 3] = [
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_36: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_37: [symbol; 7] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_38: [symbol; 7] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_39: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_40: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_41: [symbol; 4] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_2_42: [symbol; 6] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_2_43: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_2_44: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'c' as i32 as symbol,
];
static mut s_2_45: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
];
static mut s_2_46: [symbol; 3] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
];
static mut s_2_47: [symbol; 2] = ['u' as i32 as symbol, 'd' as i32 as symbol];
static mut s_2_48: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'g' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_49: [symbol; 3] = [
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_50: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_51: [symbol; 4] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_52: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_53: [symbol; 7] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_54: [symbol; 7] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_55: [symbol; 6] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_56: [symbol; 4] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_57: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_58: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_59: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_60: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_61: [symbol; 4] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_62: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_63: [symbol; 4] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_64: [symbol; 2] = ['a' as i32 as symbol, 'l' as i32 as symbol];
static mut s_2_65: [symbol; 2] = ['i' as i32 as symbol, 'l' as i32 as symbol];
static mut s_2_66: [symbol; 3] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_2_67: [symbol; 3] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_2_68: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'o' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_2_69: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_2_70: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_2_71: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_2_72: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_2_73: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_2_74: [symbol; 8] = [
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_2_75: [symbol; 4] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_2_76: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_2_77: [symbol; 2] = ['a' as i32 as symbol, 'r' as i32 as symbol];
static mut s_2_78: [symbol; 6] = [
    'i' as i32 as symbol,
    'f' as i32 as symbol,
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_79: [symbol; 4] = [
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_80: [symbol; 4] = [
    'e' as i32 as symbol,
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_81: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_82: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_83: [symbol; 3] = [
    'f' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_84: [symbol; 2] = ['o' as i32 as symbol, 'r' as i32 as symbol];
static mut s_2_85: [symbol; 3] = [
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_86: [symbol; 3] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_87: [symbol; 5] = [
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_88: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_89: [symbol; 7] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_90: [symbol; 3] = [
    'u' as i32 as symbol,
    'd' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_91: [symbol; 4] = [
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_92: [symbol; 4] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_93: [symbol; 6] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_94: [symbol; 6] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_95: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_96: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_97: [symbol; 6] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_98: [symbol; 5] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_99: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_100: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_101: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_102: [symbol; 8] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_103: [symbol; 4] = [
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_104: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_105: [symbol; 5] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_106: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_107: [symbol; 8] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_108: [symbol; 10] = [
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_109: [symbol; 6] = [
    'f' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_110: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_111: [symbol; 8] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_112: [symbol; 4] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_113: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_114: [symbol; 4] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_115: [symbol; 5] = [
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_116: [symbol; 6] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_117: [symbol; 5] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_118: [symbol; 4] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_119: [symbol; 4] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_120: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_121: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_122: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_123: [symbol; 4] = [
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_124: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_125: [symbol; 8] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_126: [symbol; 8] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_127: [symbol; 5] = [
    'i' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_128: [symbol; 9] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_129: [symbol; 4] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_130: [symbol; 6] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_131: [symbol; 7] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_132: [symbol; 10] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_133: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_134: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_135: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_136: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_137: [symbol; 5] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_138: [symbol; 2] = ['l' as i32 as symbol, 's' as i32 as symbol];
static mut s_2_139: [symbol; 3] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_140: [symbol; 4] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_141: [symbol; 3] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_142: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_143: [symbol; 9] = [
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_144: [symbol; 4] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_145: [symbol; 5] = [
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_146: [symbol; 6] = [
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_147: [symbol; 4] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_148: [symbol; 4] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_149: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_150: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_151: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_152: [symbol; 3] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_153: [symbol; 4] = [
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_154: [symbol; 5] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_155: [symbol; 5] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_156: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_157: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_158: [symbol; 8] = [
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_159: [symbol; 7] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_160: [symbol; 9] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_161: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_162: [symbol; 3] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_163: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_164: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_165: [symbol; 5] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_166: [symbol; 6] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_167: [symbol; 3] = [
    'o' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_168: [symbol; 3] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_169: [symbol; 3] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_170: [symbol; 5] = [
    't' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_171: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_172: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_2_173: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_2_174: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_2_175: [symbol; 4] = [
    'd' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_2_176: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_2_177: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_178: [symbol; 7] = [
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_179: [symbol; 6] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_180: [symbol; 8] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_181: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_182: [symbol; 2] = ['e' as i32 as symbol, 't' as i32 as symbol];
static mut s_2_183: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_184: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_185: [symbol; 4] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_186: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_187: [symbol; 5] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_188: [symbol; 7] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_189: [symbol; 2] = ['o' as i32 as symbol, 't' as i32 as symbol];
static mut s_2_190: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_2_191: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_2_192: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_2_193: [symbol; 4] = [
    't' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_2_194: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_2_195: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_2_196: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_2_197: [symbol; 3] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_2_198: [symbol; 4] = [
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_2_199: [symbol; 5] = [
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 200] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_2_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_25 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_37 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_38 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_40 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_44 as *const symbol,
            substring_i: 43 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_46 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_47 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_50 as *const symbol,
            substring_i: 49 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_51 as *const symbol,
            substring_i: 49 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_53 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_54 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_55 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_56 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_57 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_58 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_59 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_61 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_62 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_63 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_64 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_65 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_66 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_67 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_68 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_69 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_70 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_71 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_72 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_73 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_74 as *const symbol,
            substring_i: 73 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_75 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_76 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_77 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_78 as *const symbol,
            substring_i: 77 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_79 as *const symbol,
            substring_i: 77 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_80 as *const symbol,
            substring_i: 77 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_81 as *const symbol,
            substring_i: 77 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_82 as *const symbol,
            substring_i: 77 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_83 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_84 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_85 as *const symbol,
            substring_i: 84 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_86 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_87 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_88 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_89 as *const symbol,
            substring_i: 88 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_90 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_91 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_92 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_93 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_94 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_95 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_96 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_97 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_98 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_99 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_100 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_101 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_102 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_103 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_104 as *const symbol,
            substring_i: 103 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_105 as *const symbol,
            substring_i: 103 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_106 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_107 as *const symbol,
            substring_i: 106 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_2_108 as *const symbol,
            substring_i: 107 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_109 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_110 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_111 as *const symbol,
            substring_i: 110 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_112 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_113 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_114 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_115 as *const symbol,
            substring_i: 114 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_116 as *const symbol,
            substring_i: 115 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_117 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_118 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_119 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_120 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_121 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_122 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_123 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_124 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_125 as *const symbol,
            substring_i: 124 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_126 as *const symbol,
            substring_i: 124 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_127 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_2_128 as *const symbol,
            substring_i: 127 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_129 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_130 as *const symbol,
            substring_i: 129 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_131 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_2_132 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_133 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_134 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_135 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_136 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_137 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_138 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_139 as *const symbol,
            substring_i: 138 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_140 as *const symbol,
            substring_i: 138 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_141 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_142 as *const symbol,
            substring_i: 141 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_2_143 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_144 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_145 as *const symbol,
            substring_i: 144 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_146 as *const symbol,
            substring_i: 145 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_147 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_148 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_149 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_150 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_151 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_152 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_153 as *const symbol,
            substring_i: 152 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_154 as *const symbol,
            substring_i: 153 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_155 as *const symbol,
            substring_i: 153 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_156 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_157 as *const symbol,
            substring_i: 156 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_158 as *const symbol,
            substring_i: 157 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_159 as *const symbol,
            substring_i: 157 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_2_160 as *const symbol,
            substring_i: 159 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_161 as *const symbol,
            substring_i: 156 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_162 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_163 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_164 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_165 as *const symbol,
            substring_i: 164 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_166 as *const symbol,
            substring_i: 165 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_167 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_168 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_169 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_170 as *const symbol,
            substring_i: 169 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_171 as *const symbol,
            substring_i: 169 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_172 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_173 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_174 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_175 as *const symbol,
            substring_i: 174 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_176 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_177 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_178 as *const symbol,
            substring_i: 177 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_179 as *const symbol,
            substring_i: 177 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_180 as *const symbol,
            substring_i: 179 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_181 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_182 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_183 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_184 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_185 as *const symbol,
            substring_i: 184 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_186 as *const symbol,
            substring_i: 184 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_187 as *const symbol,
            substring_i: 186 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_188 as *const symbol,
            substring_i: 187 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_189 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_190 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_191 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_192 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_193 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_194 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_195 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_196 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_197 as *const symbol,
            substring_i: 196 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_198 as *const symbol,
            substring_i: 197 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_199 as *const symbol,
            substring_i: 198 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 3] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_1: [symbol; 4] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_2: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_3: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_4: [symbol; 3] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_5: [symbol; 3] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_6: [symbol; 3] = [
    'u' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_7: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_8: [symbol; 2] = ['i' as i32 as symbol, 'a' as i32 as symbol];
static mut s_3_9: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_10: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_11: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_12: [symbol; 4] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_13: [symbol; 3] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_14: [symbol; 5] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_15: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_16: [symbol; 3] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_17: [symbol; 3] = [
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_18: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_19: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_3_20: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_3_21: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_3_22: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_3_23: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_3_24: [symbol; 3] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
];
static mut s_3_25: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
];
static mut s_3_26: [symbol; 2] = ['a' as i32 as symbol, 'd' as i32 as symbol];
static mut s_3_27: [symbol; 2] = ['e' as i32 as symbol, 'd' as i32 as symbol];
static mut s_3_28: [symbol; 2] = ['i' as i32 as symbol, 'd' as i32 as symbol];
static mut s_3_29: [symbol; 2] = ['i' as i32 as symbol, 'e' as i32 as symbol];
static mut s_3_30: [symbol; 2] = ['r' as i32 as symbol, 'e' as i32 as symbol];
static mut s_3_31: [symbol; 3] = [
    'd' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_32: [symbol; 3] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_33: [symbol; 4] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_34: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_35: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_36: [symbol; 2] = ['i' as i32 as symbol, 'i' as i32 as symbol];
static mut s_3_37: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_38: [symbol; 5] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_39: [symbol; 4] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_40: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_41: [symbol; 2] = ['a' as i32 as symbol, 'm' as i32 as symbol];
static mut s_3_42: [symbol; 2] = ['e' as i32 as symbol, 'm' as i32 as symbol];
static mut s_3_43: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_44: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_45: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_46: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_47: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_48: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_49: [symbol; 5] = [
    'i' as i32 as symbol,
    'g' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_50: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'g' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_51: [symbol; 4] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_52: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_53: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_54: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_55: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_56: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_57: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_58: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_59: [symbol; 5] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_60: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_61: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_62: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_63: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_64: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_65: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_3_66: [symbol; 2] = ['a' as i32 as symbol, 'n' as i32 as symbol];
static mut s_3_67: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_68: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_69: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_70: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_71: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_72: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_73: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_74: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_75: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_76: [symbol; 2] = ['e' as i32 as symbol, 'n' as i32 as symbol];
static mut s_3_77: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_78: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_79: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_80: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_81: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_82: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_83: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_84: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_85: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_86: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_87: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_88: [symbol; 5] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_89: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_90: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_91: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_92: [symbol; 6] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_93: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_94: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_95: [symbol; 4] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_96: [symbol; 4] = [
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_97: [symbol; 5] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_98: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'x' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_99: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_100: [symbol; 2] = ['i' as i32 as symbol, 'n' as i32 as symbol];
static mut s_3_101: [symbol; 4] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_102: [symbol; 3] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_103: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_104: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_105: [symbol; 5] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_106: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_107: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_108: [symbol; 6] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_109: [symbol; 5] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_110: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_111: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_112: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_3_113: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_3_114: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_3_115: [symbol; 4] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_3_116: [symbol; 3] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_117: [symbol; 3] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_118: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_119: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_120: [symbol; 2] = ['i' as i32 as symbol, 'o' as i32 as symbol];
static mut s_3_121: [symbol; 3] = [
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_122: [symbol; 4] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_123: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'x' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_124: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_125: [symbol; 2] = ['a' as i32 as symbol, 'r' as i32 as symbol];
static mut s_3_126: [symbol; 4] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_127: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_3_128: [symbol; 5] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_129: [symbol; 2] = ['i' as i32 as symbol, 'r' as i32 as symbol];
static mut s_3_130: [symbol; 4] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_131: [symbol; 2] = ['a' as i32 as symbol, 's' as i32 as symbol];
static mut s_3_132: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_133: [symbol; 4] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_134: [symbol; 4] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_135: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_136: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_137: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_138: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_139: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_140: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_141: [symbol; 3] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_142: [symbol; 2] = ['e' as i32 as symbol, 's' as i32 as symbol];
static mut s_3_143: [symbol; 4] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_144: [symbol; 4] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_145: [symbol; 4] = [
    'u' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_146: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_147: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_148: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_149: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_150: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_151: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_152: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_153: [symbol; 6] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_154: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_155: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_156: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_157: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_158: [symbol; 5] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_159: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_160: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_161: [symbol; 4] = [
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_162: [symbol; 6] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_163: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_164: [symbol; 4] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_165: [symbol; 4] = [
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_166: [symbol; 5] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_167: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'x' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_168: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_169: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_170: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_171: [symbol; 6] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_172: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_173: [symbol; 7] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_174: [symbol; 7] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_175: [symbol; 7] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_176: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_177: [symbol; 6] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_178: [symbol; 6] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_179: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_180: [symbol; 4] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_181: [symbol; 3] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_182: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_183: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_184: [symbol; 5] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_185: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_186: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_187: [symbol; 6] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_188: [symbol; 5] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'x' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_189: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_190: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_191: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_192: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_193: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_194: [symbol; 3] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_195: [symbol; 4] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_196: [symbol; 4] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_197: [symbol; 4] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_198: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_199: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_200: [symbol; 8] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_201: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_202: [symbol; 8] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_203: [symbol; 8] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_204: [symbol; 8] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_205: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_206: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_207: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_208: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_209: [symbol; 8] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_210: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_211: [symbol; 5] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_212: [symbol; 3] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_213: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_214: [symbol; 3] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_215: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_216: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_217: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_218: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_219: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_220: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_221: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_222: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_223: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_224: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_225: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_226: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_227: [symbol; 4] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_3_228: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_3_229: [symbol; 2] = ['i' as i32 as symbol, 't' as i32 as symbol];
static mut s_3_230: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_3_231: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_3_232: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_3_233: [symbol; 2] = ['u' as i32 as symbol, 't' as i32 as symbol];
static mut s_3_234: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
];
static mut s_3_235: [symbol; 2] = ['a' as i32 as symbol, 'u' as i32 as symbol];
static mut s_3_236: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_237: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_238: [symbol; 4] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_239: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_240: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_241: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_242: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_243: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_244: [symbol; 5] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_245: [symbol; 7] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_246: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_247: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_248: [symbol; 5] = [
    'i' as i32 as symbol,
    'g' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_249: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'g' as i32 as symbol,
    'u' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_250: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_251: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_252: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_253: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_254: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_255: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_256: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_257: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_258: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_259: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_260: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_261: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_262: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_263: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3_264: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'u' as i32 as symbol,
];
static mut s_3_265: [symbol; 2] = ['i' as i32 as symbol, 'x' as i32 as symbol];
static mut s_3_266: [symbol; 3] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'x' as i32 as symbol,
];
static mut s_3_267: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'x' as i32 as symbol,
];
static mut s_3_268: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_3_269: [symbol; 3] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_3_270: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_3_271: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_3_272: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_3_273: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_3_274: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_3_275: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_3_276: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_3_277: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_3_278: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_3_279: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_3_280: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
];
static mut s_3_281: [symbol; 3] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_3_282: [symbol; 3] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut a_3: [among; 283] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_10 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_21 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_22 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_31 as *const symbol,
            substring_i: 30 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_43 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_44 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_45 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_46 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_47 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_48 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_49 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_50 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_51 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_52 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_53 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_54 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_55 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_56 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_57 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_58 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_59 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_61 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_62 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_63 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_64 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_65 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_66 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_67 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_68 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_69 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_70 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_71 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_72 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_73 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_74 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_75 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_76 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_77 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_78 as *const symbol,
            substring_i: 77 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_79 as *const symbol,
            substring_i: 77 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_80 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_81 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_82 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_83 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_84 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_85 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_86 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_87 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_88 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_89 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_90 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_91 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_92 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_93 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_94 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_95 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_96 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_97 as *const symbol,
            substring_i: 96 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_98 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_99 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_100 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_101 as *const symbol,
            substring_i: 100 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_102 as *const symbol,
            substring_i: 100 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_103 as *const symbol,
            substring_i: 102 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_104 as *const symbol,
            substring_i: 102 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_105 as *const symbol,
            substring_i: 102 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_106 as *const symbol,
            substring_i: 102 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_107 as *const symbol,
            substring_i: 102 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_108 as *const symbol,
            substring_i: 100 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_109 as *const symbol,
            substring_i: 100 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_110 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_111 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_112 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_113 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_114 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_115 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_116 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_117 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_118 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_119 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_120 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_121 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_122 as *const symbol,
            substring_i: 121 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_123 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_124 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_125 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_126 as *const symbol,
            substring_i: 125 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_127 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_128 as *const symbol,
            substring_i: 127 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_129 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_130 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_131 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_132 as *const symbol,
            substring_i: 131 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_133 as *const symbol,
            substring_i: 131 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_134 as *const symbol,
            substring_i: 131 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_135 as *const symbol,
            substring_i: 131 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_136 as *const symbol,
            substring_i: 131 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_137 as *const symbol,
            substring_i: 131 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_138 as *const symbol,
            substring_i: 137 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_139 as *const symbol,
            substring_i: 137 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_140 as *const symbol,
            substring_i: 137 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_141 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_142 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_143 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_144 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_145 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_146 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_147 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_148 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_149 as *const symbol,
            substring_i: 148 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_150 as *const symbol,
            substring_i: 148 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_151 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_152 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_153 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_154 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_155 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_156 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_157 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_158 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_159 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_160 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_161 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_162 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_163 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_164 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_165 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_166 as *const symbol,
            substring_i: 165 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_167 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_168 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_169 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_170 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_171 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_172 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_173 as *const symbol,
            substring_i: 172 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_174 as *const symbol,
            substring_i: 172 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_175 as *const symbol,
            substring_i: 172 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_176 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_177 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_178 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_179 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_180 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_181 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_182 as *const symbol,
            substring_i: 181 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_183 as *const symbol,
            substring_i: 181 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_184 as *const symbol,
            substring_i: 181 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_185 as *const symbol,
            substring_i: 181 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_186 as *const symbol,
            substring_i: 181 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_187 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_188 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_189 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_190 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_191 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_192 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_193 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_194 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_195 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_196 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_197 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_198 as *const symbol,
            substring_i: 197 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_199 as *const symbol,
            substring_i: 197 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_200 as *const symbol,
            substring_i: 197 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_201 as *const symbol,
            substring_i: 197 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_202 as *const symbol,
            substring_i: 201 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_203 as *const symbol,
            substring_i: 201 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_204 as *const symbol,
            substring_i: 201 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_205 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_206 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_207 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_208 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_209 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_210 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_211 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_212 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_213 as *const symbol,
            substring_i: 212 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_214 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_215 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_216 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_217 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_218 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_219 as *const symbol,
            substring_i: 218 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_220 as *const symbol,
            substring_i: 218 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_221 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_222 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_223 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_224 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_225 as *const symbol,
            substring_i: 224 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_226 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_227 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_228 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_229 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_230 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_231 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_232 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_233 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_234 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_235 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_236 as *const symbol,
            substring_i: 235 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_237 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_238 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_239 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_240 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_241 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_242 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_243 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_244 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_245 as *const symbol,
            substring_i: 244 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_246 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_247 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_248 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_249 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_250 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_251 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_252 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_253 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_254 as *const symbol,
            substring_i: 253 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_255 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_256 as *const symbol,
            substring_i: 255 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_257 as *const symbol,
            substring_i: 255 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_258 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_259 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_260 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_261 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_262 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_263 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_264 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_265 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_266 as *const symbol,
            substring_i: 265 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_267 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_268 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_269 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_270 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_271 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_272 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_273 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_274 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_275 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_276 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_277 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_278 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_279 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_280 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_281 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_282 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_4_1: [symbol; 1] = ['e' as i32 as symbol];
static mut s_4_2: [symbol; 1] = ['i' as i32 as symbol];
static mut s_4_3: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_4_4: [symbol; 1] = ['o' as i32 as symbol];
static mut s_4_5: [symbol; 2] = ['i' as i32 as symbol, 'r' as i32 as symbol];
static mut s_4_6: [symbol; 1] = ['s' as i32 as symbol];
static mut s_4_7: [symbol; 2] = ['i' as i32 as symbol, 's' as i32 as symbol];
static mut s_4_8: [symbol; 2] = ['o' as i32 as symbol, 's' as i32 as symbol];
static mut s_4_9: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_4_10: [symbol; 2] = ['i' as i32 as symbol, 't' as i32 as symbol];
static mut s_4_11: [symbol; 2] = ['e' as i32 as symbol, 'u' as i32 as symbol];
static mut s_4_12: [symbol; 2] = ['i' as i32 as symbol, 'u' as i32 as symbol];
static mut s_4_13: [symbol; 3] = [
    'i' as i32 as symbol,
    'q' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_4_14: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_4_15: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_4_16: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_4_17: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_4_18: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_4_19: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
];
static mut s_4_20: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_4_21: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 22] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_8 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_9 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 20] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
    129 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    81 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    6 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    10 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_1: [symbol; 1] = ['e' as i32 as symbol];
static mut s_2: [symbol; 1] = ['i' as i32 as symbol];
static mut s_3: [symbol; 1] = ['o' as i32 as symbol];
static mut s_4: [symbol; 1] = ['u' as i32 as symbol];
static mut s_5: [symbol; 1] = ['.' as i32 as symbol];
static mut s_6: [symbol; 3] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_7: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_8: [symbol; 1] = ['c' as i32 as symbol];
static mut s_9: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = out_grouping_U(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        252 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret < 0 as ::core::ffi::c_int) {
        (*z).c += ret;
        let mut ret_0: ::core::ffi::c_int = in_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            252 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_0 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_0;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
            let mut ret_1: ::core::ffi::c_int = out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                252 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_1 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_1;
                let mut ret_2: ::core::ffi::c_int = in_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    252 as ::core::ffi::c_int,
                    1 as ::core::ffi::c_int,
                );
                if !(ret_2 < 0 as ::core::ffi::c_int) {
                    (*z).c += ret_2;
                    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
                }
            }
        }
    }
    (*z).c = c1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_cleaning(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    loop {
        let mut c1: ::core::ffi::c_int = (*z).c;
        (*z).bra = (*z).c;
        if (*z).c + 1 as ::core::ffi::c_int >= (*z).l
            || *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                != 5 as ::core::ffi::c_int
            || 344765187 as ::core::ffi::c_int
                >> (*(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                & 1 as ::core::ffi::c_int == 0
        {
            among_var = 7 as ::core::ffi::c_int;
        } else {
            among_var = find_among(
                z,
                &raw const a_0 as *const among,
                13 as ::core::ffi::c_int,
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
                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_5 as *const symbol,
                );
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
            }
            7 => {
                let mut ret_5: ::core::ffi::c_int = skip_utf8(
                    (*z).p,
                    (*z).c,
                    (*z).l,
                    1 as ::core::ffi::c_int,
                );
                if ret_5 < 0 as ::core::ffi::c_int {
                    (*z).c = c1;
                    break;
                } else {
                    (*z).c = ret_5;
                }
            }
            _ => {}
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_R1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(1 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_R2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(0 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_attached_pronoun(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 1634850 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_1 as *const among, 39 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_standard_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_2 as *const among,
        200 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = r_R1(z);
            if ret <= 0 as ::core::ffi::c_int {
                return ret;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = r_R2(z);
            if ret_1 <= 0 as ::core::ffi::c_int {
                return ret_1;
            }
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        3 => {
            let mut ret_3: ::core::ffi::c_int = r_R2(z);
            if ret_3 <= 0 as ::core::ffi::c_int {
                return ret_3;
            }
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_6 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        4 => {
            let mut ret_5: ::core::ffi::c_int = r_R2(z);
            if ret_5 <= 0 as ::core::ffi::c_int {
                return ret_5;
            }
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        5 => {
            let mut ret_7: ::core::ffi::c_int = r_R1(z);
            if ret_7 <= 0 as ::core::ffi::c_int {
                return ret_7;
            }
            let mut ret_8: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_8 as *const symbol,
            );
            if ret_8 < 0 as ::core::ffi::c_int {
                return ret_8;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_verb_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_3 as *const among,
        283 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = r_R1(z);
            if ret <= 0 as ::core::ffi::c_int {
                return ret;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = r_R2(z);
            if ret_1 <= 0 as ::core::ffi::c_int {
                return ret_1;
            }
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_residual_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_4 as *const among,
        22 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = r_R1(z);
            if ret <= 0 as ::core::ffi::c_int {
                return ret;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = r_R1(z);
            if ret_1 <= 0 as ::core::ffi::c_int {
                return ret_1;
            }
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_9 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn catalan_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_mark_regions(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_attached_pronoun(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).l - m1;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_standard_suffix(z);
    if ret_1 == 0 as ::core::ffi::c_int {
        (*z).c = (*z).l - m3;
        let mut ret_2: ::core::ffi::c_int = r_verb_suffix(z);
        if !(ret_2 == 0 as ::core::ffi::c_int) {
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
    } else if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1
    }
    (*z).c = (*z).l - m2;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_residual_suffix(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = (*z).l - m4;
    (*z).c = (*z).lb;
    let mut c5: ::core::ffi::c_int = (*z).c;
    let mut ret_4: ::core::ffi::c_int = r_cleaning(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    (*z).c = c5;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn catalan_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 2 as ::core::ffi::c_int);
}
pub unsafe fn catalan_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
