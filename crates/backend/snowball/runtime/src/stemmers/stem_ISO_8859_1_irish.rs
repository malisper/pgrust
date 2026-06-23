use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 2] = ['b' as i32 as symbol, '\'' as i32 as symbol];
static mut s_0_1: [symbol; 2] = ['b' as i32 as symbol, 'h' as i32 as symbol];
static mut s_0_2: [symbol; 3] = [
    'b' as i32 as symbol,
    'h' as i32 as symbol,
    'f' as i32 as symbol,
];
static mut s_0_3: [symbol; 2] = ['b' as i32 as symbol, 'p' as i32 as symbol];
static mut s_0_4: [symbol; 2] = ['c' as i32 as symbol, 'h' as i32 as symbol];
static mut s_0_5: [symbol; 2] = ['d' as i32 as symbol, '\'' as i32 as symbol];
static mut s_0_6: [symbol; 4] = [
    'd' as i32 as symbol,
    '\'' as i32 as symbol,
    'f' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_0_7: [symbol; 2] = ['d' as i32 as symbol, 'h' as i32 as symbol];
static mut s_0_8: [symbol; 2] = ['d' as i32 as symbol, 't' as i32 as symbol];
static mut s_0_9: [symbol; 2] = ['f' as i32 as symbol, 'h' as i32 as symbol];
static mut s_0_10: [symbol; 2] = ['g' as i32 as symbol, 'c' as i32 as symbol];
static mut s_0_11: [symbol; 2] = ['g' as i32 as symbol, 'h' as i32 as symbol];
static mut s_0_12: [symbol; 2] = ['h' as i32 as symbol, '-' as i32 as symbol];
static mut s_0_13: [symbol; 2] = ['m' as i32 as symbol, '\'' as i32 as symbol];
static mut s_0_14: [symbol; 2] = ['m' as i32 as symbol, 'b' as i32 as symbol];
static mut s_0_15: [symbol; 2] = ['m' as i32 as symbol, 'h' as i32 as symbol];
static mut s_0_16: [symbol; 2] = ['n' as i32 as symbol, '-' as i32 as symbol];
static mut s_0_17: [symbol; 2] = ['n' as i32 as symbol, 'd' as i32 as symbol];
static mut s_0_18: [symbol; 2] = ['n' as i32 as symbol, 'g' as i32 as symbol];
static mut s_0_19: [symbol; 2] = ['p' as i32 as symbol, 'h' as i32 as symbol];
static mut s_0_20: [symbol; 2] = ['s' as i32 as symbol, 'h' as i32 as symbol];
static mut s_0_21: [symbol; 2] = ['t' as i32 as symbol, '-' as i32 as symbol];
static mut s_0_22: [symbol; 2] = ['t' as i32 as symbol, 'h' as i32 as symbol];
static mut s_0_23: [symbol; 2] = ['t' as i32 as symbol, 's' as i32 as symbol];
static mut a_0: [among; 24] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 6] = [
    0xed as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_1: [symbol; 7] = [
    'a' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_2: [symbol; 3] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_3: [symbol; 4] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_4: [symbol; 3] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_1_5: [symbol; 4] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_1_6: [symbol; 3] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_1_7: [symbol; 4] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_1_8: [symbol; 3] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_1_9: [symbol; 4] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_1_10: [symbol; 3] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_1_11: [symbol; 4] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_1_12: [symbol; 5] = [
    0xed as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_13: [symbol; 6] = [
    'a' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_14: [symbol; 3] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
];
static mut s_1_15: [symbol; 4] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 16] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 8] = [
    0xf3 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_1: [symbol; 7] = [
    'p' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_2: [symbol; 5] = [
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_3: [symbol; 8] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_4: [symbol; 6] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_5: [symbol; 11] = [
    'g' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'f' as i32 as symbol,
    'a' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_6: [symbol; 5] = [
    'p' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_7: [symbol; 3] = [
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_2_8: [symbol; 4] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_2_9: [symbol; 7] = [
    0xf3 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_2_10: [symbol; 7] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_2_11: [symbol; 6] = [
    'p' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_2_12: [symbol; 9] = [
    'g' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'f' as i32 as symbol,
    'a' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_2_13: [symbol; 7] = [
    'p' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'g' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_2_14: [symbol; 6] = [
    0xf3 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'g' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_2_15: [symbol; 7] = [
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    0xfa as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_2_16: [symbol; 8] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    0xfa as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_2_17: [symbol; 6] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_18: [symbol; 5] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_19: [symbol; 4] = [
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_20: [symbol; 7] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_21: [symbol; 5] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_22: [symbol; 10] = [
    'g' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'f' as i32 as symbol,
    'a' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_2_23: [symbol; 9] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
];
static mut s_2_24: [symbol; 12] = [
    'g' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'f' as i32 as symbol,
    'a' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 25] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 11 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_11 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_2_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_21 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_2_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_2_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_2_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_3_1: [symbol; 5] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_3_2: [symbol; 4] = [
    0xed as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_3_3: [symbol; 5] = [
    'a' as i32 as symbol,
    0xed as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_3_4: [symbol; 3] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_3_5: [symbol; 4] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_3_6: [symbol; 5] = [
    'f' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_3_7: [symbol; 4] = [
    'f' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_3_8: [symbol; 3] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_3_9: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_10: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_11: [symbol; 3] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut a_3: [among; 12] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
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
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
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
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    4 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    2 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 1] = ['f' as i32 as symbol];
static mut s_1: [symbol; 1] = ['s' as i32 as symbol];
static mut s_2: [symbol; 1] = ['b' as i32 as symbol];
static mut s_3: [symbol; 1] = ['c' as i32 as symbol];
static mut s_4: [symbol; 1] = ['d' as i32 as symbol];
static mut s_5: [symbol; 1] = ['g' as i32 as symbol];
static mut s_6: [symbol; 1] = ['p' as i32 as symbol];
static mut s_7: [symbol; 1] = ['t' as i32 as symbol];
static mut s_8: [symbol; 1] = ['m' as i32 as symbol];
static mut s_9: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'c' as i32 as symbol,
];
static mut s_10: [symbol; 3] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11: [symbol; 4] = [
    'g' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'f' as i32 as symbol,
];
static mut s_12: [symbol; 5] = [
    'p' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_13: [symbol; 3] = [
    0xf3 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = out_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        250 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret < 0 as ::core::ffi::c_int) {
        (*z).c += ret;
        *(*z).I.offset(2 as ::core::ffi::c_int as isize) = (*z).c;
        let mut ret_0: ::core::ffi::c_int = in_grouping(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            250 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_0 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_0;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
            let mut ret_1: ::core::ffi::c_int = out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                250 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_1 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_1;
                let mut ret_2: ::core::ffi::c_int = in_grouping(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    250 as ::core::ffi::c_int,
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
unsafe fn r_initial_morph(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    among_var = find_among(z, &raw const a_0 as *const among, 24 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_0 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_1 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_2 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        5 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_3 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        6 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_4 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        7 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_5 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        8 => {
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_6 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        9 => {
            let mut ret_7: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
        }
        10 => {
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
unsafe fn r_RV(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(2 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_R1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(1 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_R2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(0 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_noun_sfx(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_1 as *const among,
        16 as ::core::ffi::c_int,
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
unsafe fn r_deriv(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_2 as *const among,
        25 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = r_R2(z);
            if ret <= 0 as ::core::ffi::c_int {
                return ret;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_9 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_10 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        4 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_11 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        5 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_12 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        6 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_13 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_verb_sfx(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 282896 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_3 as *const among,
        12 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = r_RV(z);
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
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn irish_ISO_8859_1_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_initial_morph(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = c1;
    let mut ret_0: ::core::ffi::c_int = r_mark_regions(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_noun_sfx(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_deriv(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m3;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_verb_sfx(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = (*z).l - m4;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn irish_ISO_8859_1_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 3 as ::core::ffi::c_int);
}
pub unsafe fn irish_ISO_8859_1_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
