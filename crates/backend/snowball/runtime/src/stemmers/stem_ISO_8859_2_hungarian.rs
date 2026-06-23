use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 2] = ['c' as i32 as symbol, 's' as i32 as symbol];
static mut s_0_1: [symbol; 3] = [
    'd' as i32 as symbol,
    'z' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_2: [symbol; 2] = ['g' as i32 as symbol, 'y' as i32 as symbol];
static mut s_0_3: [symbol; 2] = ['l' as i32 as symbol, 'y' as i32 as symbol];
static mut s_0_4: [symbol; 2] = ['n' as i32 as symbol, 'y' as i32 as symbol];
static mut s_0_5: [symbol; 2] = ['s' as i32 as symbol, 'z' as i32 as symbol];
static mut s_0_6: [symbol; 2] = ['t' as i32 as symbol, 'y' as i32 as symbol];
static mut s_0_7: [symbol; 2] = ['z' as i32 as symbol, 's' as i32 as symbol];
static mut a_0: [among; 8] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 1] = [0xe1 as ::core::ffi::c_int as symbol];
static mut s_1_1: [symbol; 1] = [0xe9 as ::core::ffi::c_int as symbol];
static mut a_1: [among; 2] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 2] = ['b' as i32 as symbol, 'b' as i32 as symbol];
static mut s_2_1: [symbol; 2] = ['c' as i32 as symbol, 'c' as i32 as symbol];
static mut s_2_2: [symbol; 2] = ['d' as i32 as symbol, 'd' as i32 as symbol];
static mut s_2_3: [symbol; 2] = ['f' as i32 as symbol, 'f' as i32 as symbol];
static mut s_2_4: [symbol; 2] = ['g' as i32 as symbol, 'g' as i32 as symbol];
static mut s_2_5: [symbol; 2] = ['j' as i32 as symbol, 'j' as i32 as symbol];
static mut s_2_6: [symbol; 2] = ['k' as i32 as symbol, 'k' as i32 as symbol];
static mut s_2_7: [symbol; 2] = ['l' as i32 as symbol, 'l' as i32 as symbol];
static mut s_2_8: [symbol; 2] = ['m' as i32 as symbol, 'm' as i32 as symbol];
static mut s_2_9: [symbol; 2] = ['n' as i32 as symbol, 'n' as i32 as symbol];
static mut s_2_10: [symbol; 2] = ['p' as i32 as symbol, 'p' as i32 as symbol];
static mut s_2_11: [symbol; 2] = ['r' as i32 as symbol, 'r' as i32 as symbol];
static mut s_2_12: [symbol; 3] = [
    'c' as i32 as symbol,
    'c' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_13: [symbol; 2] = ['s' as i32 as symbol, 's' as i32 as symbol];
static mut s_2_14: [symbol; 3] = [
    'z' as i32 as symbol,
    'z' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_15: [symbol; 2] = ['t' as i32 as symbol, 't' as i32 as symbol];
static mut s_2_16: [symbol; 2] = ['v' as i32 as symbol, 'v' as i32 as symbol];
static mut s_2_17: [symbol; 3] = [
    'g' as i32 as symbol,
    'g' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_2_18: [symbol; 3] = [
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_2_19: [symbol; 3] = [
    'n' as i32 as symbol,
    'n' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_2_20: [symbol; 3] = [
    't' as i32 as symbol,
    't' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_2_21: [symbol; 3] = [
    's' as i32 as symbol,
    's' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_2_22: [symbol; 2] = ['z' as i32 as symbol, 'z' as i32 as symbol];
static mut a_2: [among; 23] = unsafe {
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
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 2] = ['a' as i32 as symbol, 'l' as i32 as symbol];
static mut s_3_1: [symbol; 2] = ['e' as i32 as symbol, 'l' as i32 as symbol];
static mut a_3: [among; 2] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 2] = ['b' as i32 as symbol, 'a' as i32 as symbol];
static mut s_4_1: [symbol; 2] = ['r' as i32 as symbol, 'a' as i32 as symbol];
static mut s_4_2: [symbol; 2] = ['b' as i32 as symbol, 'e' as i32 as symbol];
static mut s_4_3: [symbol; 2] = ['r' as i32 as symbol, 'e' as i32 as symbol];
static mut s_4_4: [symbol; 2] = ['i' as i32 as symbol, 'g' as i32 as symbol];
static mut s_4_5: [symbol; 3] = [
    'n' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_4_6: [symbol; 3] = [
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_4_7: [symbol; 3] = [
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_4_8: [symbol; 3] = [
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_4_9: [symbol; 2] = ['u' as i32 as symbol, 'l' as i32 as symbol];
static mut s_4_10: [symbol; 3] = [
    'n' as i32 as symbol,
    0xe1 as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_4_11: [symbol; 3] = [
    'n' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_4_12: [symbol; 3] = [
    'b' as i32 as symbol,
    0xf3 as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_4_13: [symbol; 3] = [
    'r' as i32 as symbol,
    0xf3 as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_4_14: [symbol; 3] = [
    't' as i32 as symbol,
    0xf3 as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_4_15: [symbol; 3] = [
    'b' as i32 as symbol,
    0xf5 as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_4_16: [symbol; 3] = [
    'r' as i32 as symbol,
    0xf5 as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_4_17: [symbol; 3] = [
    't' as i32 as symbol,
    0xf5 as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_4_18: [symbol; 2] = [
    0xfc as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_4_19: [symbol; 1] = ['n' as i32 as symbol];
static mut s_4_20: [symbol; 2] = ['a' as i32 as symbol, 'n' as i32 as symbol];
static mut s_4_21: [symbol; 3] = [
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_4_22: [symbol; 2] = ['e' as i32 as symbol, 'n' as i32 as symbol];
static mut s_4_23: [symbol; 3] = [
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_4_24: [symbol; 6] = [
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'p' as i32 as symbol,
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_4_25: [symbol; 2] = ['o' as i32 as symbol, 'n' as i32 as symbol];
static mut s_4_26: [symbol; 2] = [
    0xf6 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_4_27: [symbol; 4] = [
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'p' as i32 as symbol,
    'p' as i32 as symbol,
];
static mut s_4_28: [symbol; 3] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_4_29: [symbol; 1] = ['t' as i32 as symbol];
static mut s_4_30: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_4_31: [symbol; 2] = ['e' as i32 as symbol, 't' as i32 as symbol];
static mut s_4_32: [symbol; 4] = [
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_33: [symbol; 6] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_34: [symbol; 6] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_35: [symbol; 6] = [
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_36: [symbol; 2] = ['o' as i32 as symbol, 't' as i32 as symbol];
static mut s_4_37: [symbol; 3] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_38: [symbol; 2] = [
    0xf6 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
];
static mut s_4_39: [symbol; 3] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_4_40: [symbol; 3] = [
    'h' as i32 as symbol,
    'o' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_4_41: [symbol; 3] = [
    'h' as i32 as symbol,
    0xf6 as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut s_4_42: [symbol; 2] = [
    'v' as i32 as symbol,
    0xe1 as ::core::ffi::c_int as symbol,
];
static mut s_4_43: [symbol; 2] = [
    'v' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 44] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_21 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_22 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_24 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_25 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_26 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_31 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_32 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_33 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_34 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_35 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_36 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_37 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_38 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_5_1: [symbol; 2] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_5_2: [symbol; 6] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut a_5: [among; 3] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 4] = [
    's' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_6_1: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_6_2: [symbol; 5] = [
    0xe1 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_6_3: [symbol; 4] = [
    's' as i32 as symbol,
    't' as i32 as symbol,
    0xfc as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_6_4: [symbol; 5] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    0xfc as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_6_5: [symbol; 5] = [
    0xe9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    0xfc as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut a_6: [among; 6] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_5 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 1] = [0xe1 as ::core::ffi::c_int as symbol];
static mut s_7_1: [symbol; 1] = [0xe9 as ::core::ffi::c_int as symbol];
static mut a_7: [among; 2] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_7_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_8_0: [symbol; 1] = ['k' as i32 as symbol];
static mut s_8_1: [symbol; 2] = ['a' as i32 as symbol, 'k' as i32 as symbol];
static mut s_8_2: [symbol; 2] = ['e' as i32 as symbol, 'k' as i32 as symbol];
static mut s_8_3: [symbol; 2] = ['o' as i32 as symbol, 'k' as i32 as symbol];
static mut s_8_4: [symbol; 2] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_8_5: [symbol; 2] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_8_6: [symbol; 2] = [
    0xf6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut a_8: [among; 7] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_8_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_9_0: [symbol; 2] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_9_1: [symbol; 3] = [
    0xe1 as ::core::ffi::c_int as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_9_2: [symbol; 3] = [
    0xe9 as ::core::ffi::c_int as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_9_3: [symbol; 1] = [0xe9 as ::core::ffi::c_int as symbol];
static mut s_9_4: [symbol; 2] = [
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut s_9_5: [symbol; 3] = [
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut s_9_6: [symbol; 3] = [
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut s_9_7: [symbol; 3] = [
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut s_9_8: [symbol; 3] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut s_9_9: [symbol; 3] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut s_9_10: [symbol; 3] = [
    0xf6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut s_9_11: [symbol; 2] = [
    0xe9 as ::core::ffi::c_int as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut a_9: [among; 12] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_9_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_6 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_7 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_8 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_9 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_10 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_11 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_10_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_10_1: [symbol; 2] = ['j' as i32 as symbol, 'a' as i32 as symbol];
static mut s_10_2: [symbol; 1] = ['d' as i32 as symbol];
static mut s_10_3: [symbol; 2] = ['a' as i32 as symbol, 'd' as i32 as symbol];
static mut s_10_4: [symbol; 2] = ['e' as i32 as symbol, 'd' as i32 as symbol];
static mut s_10_5: [symbol; 2] = ['o' as i32 as symbol, 'd' as i32 as symbol];
static mut s_10_6: [symbol; 2] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
];
static mut s_10_7: [symbol; 2] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
];
static mut s_10_8: [symbol; 2] = [
    0xf6 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
];
static mut s_10_9: [symbol; 1] = ['e' as i32 as symbol];
static mut s_10_10: [symbol; 2] = ['j' as i32 as symbol, 'e' as i32 as symbol];
static mut s_10_11: [symbol; 2] = ['n' as i32 as symbol, 'k' as i32 as symbol];
static mut s_10_12: [symbol; 3] = [
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_10_13: [symbol; 3] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_10_14: [symbol; 3] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_10_15: [symbol; 3] = [
    0xfc as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_10_16: [symbol; 2] = ['u' as i32 as symbol, 'k' as i32 as symbol];
static mut s_10_17: [symbol; 3] = [
    'j' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_10_18: [symbol; 4] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_10_19: [symbol; 2] = [
    0xfc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_10_20: [symbol; 3] = [
    'j' as i32 as symbol,
    0xfc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_10_21: [symbol; 4] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
    0xfc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_10_22: [symbol; 1] = ['m' as i32 as symbol];
static mut s_10_23: [symbol; 2] = ['a' as i32 as symbol, 'm' as i32 as symbol];
static mut s_10_24: [symbol; 2] = ['e' as i32 as symbol, 'm' as i32 as symbol];
static mut s_10_25: [symbol; 2] = ['o' as i32 as symbol, 'm' as i32 as symbol];
static mut s_10_26: [symbol; 2] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_10_27: [symbol; 2] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_10_28: [symbol; 1] = ['o' as i32 as symbol];
static mut s_10_29: [symbol; 1] = [0xe1 as ::core::ffi::c_int as symbol];
static mut s_10_30: [symbol; 1] = [0xe9 as ::core::ffi::c_int as symbol];
static mut a_10: [among; 31] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_10_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_10_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_4 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_5 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_6 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_7 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_8 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_10_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_12 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_13 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_14 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_15 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_21 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_10_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_24 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_25 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_26 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_27 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_10_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_10_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_10_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_11_0: [symbol; 2] = ['i' as i32 as symbol, 'd' as i32 as symbol];
static mut s_11_1: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_2: [symbol; 4] = [
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_3: [symbol; 3] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_4: [symbol; 4] = [
    'j' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_5: [symbol; 3] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_6: [symbol; 3] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_7: [symbol; 1] = ['i' as i32 as symbol];
static mut s_11_8: [symbol; 2] = ['a' as i32 as symbol, 'i' as i32 as symbol];
static mut s_11_9: [symbol; 3] = [
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_10: [symbol; 2] = ['e' as i32 as symbol, 'i' as i32 as symbol];
static mut s_11_11: [symbol; 3] = [
    'j' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_12: [symbol; 2] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_11_13: [symbol; 2] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_11_14: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_15: [symbol; 5] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_16: [symbol; 6] = [
    'j' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_17: [symbol; 5] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_18: [symbol; 2] = ['i' as i32 as symbol, 'k' as i32 as symbol];
static mut s_11_19: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_20: [symbol; 4] = [
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_21: [symbol; 3] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_22: [symbol; 4] = [
    'j' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_23: [symbol; 3] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_24: [symbol; 3] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_25: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_26: [symbol; 4] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_27: [symbol; 5] = [
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_28: [symbol; 4] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_29: [symbol; 5] = [
    'j' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_30: [symbol; 4] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_31: [symbol; 4] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_32: [symbol; 5] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_33: [symbol; 6] = [
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_34: [symbol; 5] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_11_35: [symbol; 2] = ['i' as i32 as symbol, 'm' as i32 as symbol];
static mut s_11_36: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_11_37: [symbol; 4] = [
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_11_38: [symbol; 3] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_11_39: [symbol; 4] = [
    'j' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_11_40: [symbol; 3] = [
    0xe1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_11_41: [symbol; 3] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut a_11: [among; 42] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_11_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_10 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_12 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_13 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_17 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_21 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_22 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_23 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_24 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_28 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_29 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_30 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_31 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_33 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_36 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_37 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_38 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_39 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_40 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_41 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
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
    52 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    14 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_1: [symbol; 1] = ['e' as i32 as symbol];
static mut s_2: [symbol; 1] = ['e' as i32 as symbol];
static mut s_3: [symbol; 1] = ['a' as i32 as symbol];
static mut s_4: [symbol; 1] = ['a' as i32 as symbol];
static mut s_5: [symbol; 1] = ['e' as i32 as symbol];
static mut s_6: [symbol; 1] = ['a' as i32 as symbol];
static mut s_7: [symbol; 1] = ['e' as i32 as symbol];
static mut s_8: [symbol; 1] = ['e' as i32 as symbol];
static mut s_9: [symbol; 1] = ['a' as i32 as symbol];
static mut s_10: [symbol; 1] = ['a' as i32 as symbol];
static mut s_11: [symbol; 1] = ['e' as i32 as symbol];
static mut s_12: [symbol; 1] = ['a' as i32 as symbol];
static mut s_13: [symbol; 1] = ['e' as i32 as symbol];
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c1: ::core::ffi::c_int = (*z).c;
    if in_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        252 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 6258161466625774711;
    } else if in_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        252 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    ) < 0 as ::core::ffi::c_int
    {
        current_block = 6258161466625774711;
    } else {
        let mut c2: ::core::ffi::c_int = (*z).c;
        if (*z).c + 1 as ::core::ffi::c_int >= (*z).l
            || *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                != 3 as ::core::ffi::c_int
            || 101187584 as ::core::ffi::c_int
                >> (*(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                & 1 as ::core::ffi::c_int == 0
        {
            current_block = 8438879142660041743;
        } else if find_among(z, &raw const a_0 as *const among, 8 as ::core::ffi::c_int)
            == 0
        {
            current_block = 8438879142660041743;
        } else {
            current_block = 5442517853276296404;
        }
        match current_block {
            8438879142660041743 => {
                (*z).c = c2;
                if (*z).c >= (*z).l {
                    current_block = 6258161466625774711;
                } else {
                    (*z).c += 1;
                    current_block = 5442517853276296404;
                }
            }
            _ => {}
        }
        match current_block {
            6258161466625774711 => {}
            _ => {
                *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
                current_block = 462489325082369757;
            }
        }
    }
    match current_block {
        6258161466625774711 => {
            (*z).c = c1;
            if out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                252 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                252 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c += ret;
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_R1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(0 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_v_ending(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 225 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 233 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_1 as *const among, 2 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_0 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_1 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_double(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m_test1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 106790108 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_2 as *const among, 23 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c = (*z).l - m_test1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_undouble(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if (*z).c <= (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= 1;
    (*z).ket = (*z).c;
    (*z).c = (*z).c - 1 as ::core::ffi::c_int;
    if (*z).c < (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_instrum(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 108 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_3 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut ret_0: ::core::ffi::c_int = r_double(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    let mut ret_1: ::core::ffi::c_int = slice_del(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    let mut ret_2: ::core::ffi::c_int = r_undouble(z);
    if ret_2 <= 0 as ::core::ffi::c_int {
        return ret_2;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_case(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_4 as *const among, 44 as ::core::ffi::c_int) == 0 {
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
    let mut ret_1: ::core::ffi::c_int = r_v_ending(z);
    if ret_1 <= 0 as ::core::ffi::c_int {
        return ret_1;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_case_special(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 110 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 116 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_5 as *const among, 3 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_2 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_3 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_case_other(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 108 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_6 as *const among, 6 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_4 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_5 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_factive(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 225 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 233 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_7 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut ret_0: ::core::ffi::c_int = r_double(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    let mut ret_1: ::core::ffi::c_int = slice_del(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    let mut ret_2: ::core::ffi::c_int = r_undouble(z);
    if ret_2 <= 0 as ::core::ffi::c_int {
        return ret_2;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_plural(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 107 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_8 as *const among, 7 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_6 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_owned(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 105 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 233 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_9 as *const among,
        12 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_8 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
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
unsafe fn r_sing_owner(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_10 as *const among,
        31 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_10 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_11 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_plur_owner(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 10768 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_11 as *const among,
        42 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R1(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_12 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_13 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn hungarian_ISO_8859_2_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_mark_regions(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = c1;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_instrum(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_case(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m3;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_case_special(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m4;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_case_other(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = (*z).l - m5;
    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_4: ::core::ffi::c_int = r_factive(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    (*z).c = (*z).l - m6;
    let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_5: ::core::ffi::c_int = r_owned(z);
    if ret_5 < 0 as ::core::ffi::c_int {
        return ret_5;
    }
    (*z).c = (*z).l - m7;
    let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_6: ::core::ffi::c_int = r_sing_owner(z);
    if ret_6 < 0 as ::core::ffi::c_int {
        return ret_6;
    }
    (*z).c = (*z).l - m8;
    let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_7: ::core::ffi::c_int = r_plur_owner(z);
    if ret_7 < 0 as ::core::ffi::c_int {
        return ret_7;
    }
    (*z).c = (*z).l - m9;
    let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_8: ::core::ffi::c_int = r_plural(z);
    if ret_8 < 0 as ::core::ffi::c_int {
        return ret_8;
    }
    (*z).c = (*z).l - m10;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn hungarian_ISO_8859_2_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 1 as ::core::ffi::c_int);
}
pub unsafe fn hungarian_ISO_8859_2_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
