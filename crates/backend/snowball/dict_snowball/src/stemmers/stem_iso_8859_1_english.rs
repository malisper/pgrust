use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_1: [symbol; 6] = [
    'c' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_2: [symbol; 5] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut a_0: [among; 3] = unsafe {
    [
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 1] = ['\'' as i32 as symbol];
static mut s_1_1: [symbol; 3] = [
    '\'' as i32 as symbol,
    's' as i32 as symbol,
    '\'' as i32 as symbol,
];
static mut s_1_2: [symbol; 2] = ['\'' as i32 as symbol, 's' as i32 as symbol];
static mut a_1: [among; 3] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_2_1: [symbol; 1] = ['s' as i32 as symbol];
static mut s_2_2: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_3: [symbol; 4] = [
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_4: [symbol; 2] = ['s' as i32 as symbol, 's' as i32 as symbol];
static mut s_2_5: [symbol; 2] = ['u' as i32 as symbol, 's' as i32 as symbol];
static mut a_2: [among; 6] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_3_1: [symbol; 2] = ['b' as i32 as symbol, 'b' as i32 as symbol];
static mut s_3_2: [symbol; 2] = ['d' as i32 as symbol, 'd' as i32 as symbol];
static mut s_3_3: [symbol; 2] = ['f' as i32 as symbol, 'f' as i32 as symbol];
static mut s_3_4: [symbol; 2] = ['g' as i32 as symbol, 'g' as i32 as symbol];
static mut s_3_5: [symbol; 2] = ['b' as i32 as symbol, 'l' as i32 as symbol];
static mut s_3_6: [symbol; 2] = ['m' as i32 as symbol, 'm' as i32 as symbol];
static mut s_3_7: [symbol; 2] = ['n' as i32 as symbol, 'n' as i32 as symbol];
static mut s_3_8: [symbol; 2] = ['p' as i32 as symbol, 'p' as i32 as symbol];
static mut s_3_9: [symbol; 2] = ['r' as i32 as symbol, 'r' as i32 as symbol];
static mut s_3_10: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_3_11: [symbol; 2] = ['t' as i32 as symbol, 't' as i32 as symbol];
static mut s_3_12: [symbol; 2] = ['i' as i32 as symbol, 'z' as i32 as symbol];
static mut a_3: [among; 13] = unsafe {
    [
        among {
            s_size: 0 as ::core::ffi::c_int,
            s: ::core::ptr::null::<symbol>(),
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_7 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_8 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_9 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_10 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_11 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_12 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 2] = ['e' as i32 as symbol, 'd' as i32 as symbol];
static mut s_4_1: [symbol; 3] = [
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_4_2: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_4_3: [symbol; 4] = [
    'e' as i32 as symbol,
    'd' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_4_4: [symbol; 5] = [
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_4_5: [symbol; 5] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut a_4: [among; 6] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_1: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_2: [symbol; 3] = [
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_3: [symbol; 2] = ['l' as i32 as symbol, 'i' as i32 as symbol];
static mut s_5_4: [symbol; 3] = [
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_5: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_6: [symbol; 4] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_7: [symbol; 5] = [
    'f' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_8: [symbol; 6] = [
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_9: [symbol; 5] = [
    'o' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_10: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_11: [symbol; 5] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_12: [symbol; 6] = [
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_13: [symbol; 5] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_14: [symbol; 6] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_5_15: [symbol; 7] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_5_16: [symbol; 5] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_5_17: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_5_18: [symbol; 7] = [
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_5_19: [symbol; 4] = [
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_5_20: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_5_21: [symbol; 7] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_22: [symbol; 7] = [
    'f' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_23: [symbol; 7] = [
    'o' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut a_5: [among; 24] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_6 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_7 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_8 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_9 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_10 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_1: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_2: [symbol; 5] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_3: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_4: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_6_5: [symbol; 6] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_6_6: [symbol; 7] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_6_7: [symbol; 3] = [
    'f' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_6_8: [symbol; 4] = [
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut a_6: [among; 9] = unsafe {
    [
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_6_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_7_1: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_2: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_3: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_4: [symbol; 4] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_5: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_6: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_7: [symbol; 3] = [
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_8: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_9: [symbol; 2] = ['a' as i32 as symbol, 'l' as i32 as symbol];
static mut s_7_10: [symbol; 3] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_7_11: [symbol; 3] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_7_12: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_7_13: [symbol; 3] = [
    'o' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_7_14: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_7_15: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_7_16: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_7_17: [symbol; 5] = [
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut a_7: [among; 18] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_8_0: [symbol; 1] = ['e' as i32 as symbol];
static mut s_8_1: [symbol; 1] = ['l' as i32 as symbol];
static mut a_8: [among; 2] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_8_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_8_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_9_0: [symbol; 7] = [
    's' as i32 as symbol,
    'u' as i32 as symbol,
    'c' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_9_1: [symbol; 7] = [
    'p' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_9_2: [symbol; 6] = [
    'e' as i32 as symbol,
    'x' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_9_3: [symbol; 7] = [
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_9_4: [symbol; 6] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_9_5: [symbol; 7] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_9_6: [symbol; 7] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_9_7: [symbol; 6] = [
    'o' as i32 as symbol,
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut a_9: [among; 8] = unsafe {
    [
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_9_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_9_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_9_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_9_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_9_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_9_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_9_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_9_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_10_0: [symbol; 5] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_10_1: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_10_2: [symbol; 4] = [
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_10_3: [symbol; 6] = [
    'c' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_10_4: [symbol; 5] = [
    'd' as i32 as symbol,
    'y' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_10_5: [symbol; 5] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_10_6: [symbol; 6] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_10_7: [symbol; 4] = [
    'h' as i32 as symbol,
    'o' as i32 as symbol,
    'w' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_10_8: [symbol; 4] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_10_9: [symbol; 5] = [
    'l' as i32 as symbol,
    'y' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_10_10: [symbol; 4] = [
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'w' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_10_11: [symbol; 4] = [
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_10_12: [symbol; 6] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_10_13: [symbol; 5] = [
    's' as i32 as symbol,
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_10_14: [symbol; 4] = [
    's' as i32 as symbol,
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_10_15: [symbol; 3] = [
    's' as i32 as symbol,
    'k' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_10_16: [symbol; 5] = [
    't' as i32 as symbol,
    'y' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_10_17: [symbol; 4] = [
    'u' as i32 as symbol,
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut a_10: [among; 18] = unsafe {
    [
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_10_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_10_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_10_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_10_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_10_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_10_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_10_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_aeo: [::core::ffi::c_uchar; 2] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    64 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_v: [::core::ffi::c_uchar; 4] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_v_WXY: [::core::ffi::c_uchar; 5] = [
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    208 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_valid_LI: [::core::ffi::c_uchar; 3] = [
    55 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    141 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    2 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 1] = ['Y' as i32 as symbol];
static mut s_1: [symbol; 1] = ['Y' as i32 as symbol];
static mut s_2: [symbol; 2] = ['s' as i32 as symbol, 's' as i32 as symbol];
static mut s_3: [symbol; 1] = ['i' as i32 as symbol];
static mut s_4: [symbol; 2] = ['i' as i32 as symbol, 'e' as i32 as symbol];
static mut s_5: [symbol; 2] = ['e' as i32 as symbol, 'e' as i32 as symbol];
static mut s_6: [symbol; 1] = ['e' as i32 as symbol];
static mut s_7: [symbol; 1] = ['e' as i32 as symbol];
static mut s_8: [symbol; 1] = ['i' as i32 as symbol];
static mut s_9: [symbol; 4] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_10: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_12: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_13: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_14: [symbol; 3] = [
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_15: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_16: [symbol; 2] = ['a' as i32 as symbol, 'l' as i32 as symbol];
static mut s_17: [symbol; 3] = [
    'f' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_18: [symbol; 3] = [
    'o' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_19: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_20: [symbol; 3] = [
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_21: [symbol; 2] = ['o' as i32 as symbol, 'g' as i32 as symbol];
static mut s_22: [symbol; 4] = [
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_23: [symbol; 4] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_24: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_25: [symbol; 2] = ['a' as i32 as symbol, 'l' as i32 as symbol];
static mut s_26: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_27: [symbol; 3] = [
    's' as i32 as symbol,
    'k' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_28: [symbol; 3] = [
    's' as i32 as symbol,
    'k' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_29: [symbol; 3] = [
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_30: [symbol; 3] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_31: [symbol; 3] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_32: [symbol; 3] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_33: [symbol; 5] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_34: [symbol; 4] = [
    'u' as i32 as symbol,
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_35: [symbol; 5] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_36: [symbol; 4] = [
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_37: [symbol; 5] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_38: [symbol; 1] = ['y' as i32 as symbol];
unsafe fn r_prelude(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut c1: ::core::ffi::c_int = (*z).c;
    (*z).bra = (*z).c;
    if !((*z).c == (*z).l
        || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int != '\'' as i32)
    {
        (*z).c += 1;
        (*z).ket = (*z).c;
        let mut ret: ::core::ffi::c_int = slice_del(z);
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
    }
    (*z).c = c1;
    let mut c2: ::core::ffi::c_int = (*z).c;
    (*z).bra = (*z).c;
    if !((*z).c == (*z).l
        || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int != 'y' as i32)
    {
        (*z).c += 1;
        (*z).ket = (*z).c;
        let mut ret_0: ::core::ffi::c_int = slice_from_s(
            z,
            1 as ::core::ffi::c_int,
            &raw const s_0 as *const symbol,
        );
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
        *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    }
    (*z).c = c2;
    let mut c3: ::core::ffi::c_int = (*z).c;
    loop {
        let mut c4: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c5: ::core::ffi::c_int = (*z).c;
            if !(in_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                121 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0)
            {
                (*z).bra = (*z).c;
                if !((*z).c == (*z).l
                    || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                        != 'y' as i32)
                {
                    (*z).c += 1;
                    (*z).ket = (*z).c;
                    (*z).c = c5;
                    current_block = 14763689060501151050;
                    break;
                }
            }
            (*z).c = c5;
            if (*z).c >= (*z).l {
                current_block = 7960025966495331773;
                break;
            }
            (*z).c += 1;
        }
        match current_block {
            7960025966495331773 => {
                (*z).c = c4;
                break;
            }
            _ => {
                let mut ret_1: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_1 as *const symbol,
                );
                if ret_1 < 0 as ::core::ffi::c_int {
                    return ret_1;
                }
                *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 1
                    as ::core::ffi::c_int;
            }
        }
    }
    (*z).c = c3;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut c2: ::core::ffi::c_int = (*z).c;
    if (*z).c + 4 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 4 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 2375680 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c + 4 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        current_block = 862581995378286052;
    } else if find_among(z, &raw const a_0 as *const among, 3 as ::core::ffi::c_int) == 0
    {
        current_block = 862581995378286052;
    } else {
        current_block = 10298879842757584974;
    }
    match current_block {
        862581995378286052 => {
            (*z).c = c2;
            let mut ret: ::core::ffi::c_int = out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                121 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                current_block = 15315208969778783461;
            } else {
                (*z).c += ret;
                let mut ret_0: ::core::ffi::c_int = in_grouping(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    121 as ::core::ffi::c_int,
                    1 as ::core::ffi::c_int,
                );
                if ret_0 < 0 as ::core::ffi::c_int {
                    current_block = 15315208969778783461;
                } else {
                    (*z).c += ret_0;
                    current_block = 10298879842757584974;
                }
            }
        }
        _ => {}
    }
    match current_block {
        10298879842757584974 => {
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
            let mut ret_1: ::core::ffi::c_int = out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                121 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_1 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_1;
                let mut ret_2: ::core::ffi::c_int = in_grouping(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    121 as ::core::ffi::c_int,
                    1 as ::core::ffi::c_int,
                );
                if !(ret_2 < 0 as ::core::ffi::c_int) {
                    (*z).c += ret_2;
                    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
                }
            }
        }
        _ => {}
    }
    (*z).c = c1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_shortv(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if out_grouping_b(
        z,
        &raw const g_v_WXY as *const ::core::ffi::c_uchar,
        89 as ::core::ffi::c_int,
        121 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 10431277913278694499;
    } else if in_grouping_b(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        121 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 10431277913278694499;
    } else if out_grouping_b(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        121 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 10431277913278694499;
    } else {
        current_block = 11029340638635949224;
    }
    match current_block {
        10431277913278694499 => {
            (*z).c = (*z).l - m1;
            if out_grouping_b(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                121 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            if in_grouping_b(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                121 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            if (*z).c > (*z).lb {
                return 0 as ::core::ffi::c_int;
            }
        }
        _ => {}
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
unsafe fn r_Step_1a(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 39 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 115 as ::core::ffi::c_int
    {
        (*z).c = (*z).l - m1;
    } else if find_among_b(z, &raw const a_1 as *const among, 3 as ::core::ffi::c_int)
        == 0
    {
        (*z).c = (*z).l - m1;
    } else {
        (*z).bra = (*z).c;
        let mut ret: ::core::ffi::c_int = slice_del(z);
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
    }
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 100 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 115 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_2 as *const among, 6 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_2 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).c = (*z).c - 2 as ::core::ffi::c_int;
            if (*z).c < (*z).lb {
                (*z).c = (*z).l - m2;
                let mut ret_2: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_4 as *const symbol,
                );
                if ret_2 < 0 as ::core::ffi::c_int {
                    return ret_2;
                }
            } else {
                let mut ret_1: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_3 as *const symbol,
                );
                if ret_1 < 0 as ::core::ffi::c_int {
                    return ret_1;
                }
            }
        }
        3 => {
            if (*z).c <= (*z).lb {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
            let mut ret_3: ::core::ffi::c_int = out_grouping_b(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                121 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= ret_3;
            let mut ret_4: ::core::ffi::c_int = slice_del(z);
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Step_1b(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 33554576 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_4 as *const among, 6 as ::core::ffi::c_int);
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
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_5 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut m_test1: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_1: ::core::ffi::c_int = out_grouping_b(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                121 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= ret_1;
            (*z).c = (*z).l - m_test1;
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                    != 3 as ::core::ffi::c_int
                || 68514004 as ::core::ffi::c_int
                    >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                    & 1 as ::core::ffi::c_int == 0
            {
                among_var = 3 as ::core::ffi::c_int;
            } else {
                among_var = find_among_b(
                    z,
                    &raw const a_3 as *const among,
                    13 as ::core::ffi::c_int,
                );
            }
            match among_var {
                1 => {
                    let mut ret_3: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_6 as *const symbol,
                    );
                    if ret_3 < 0 as ::core::ffi::c_int {
                        return ret_3;
                    }
                    return 0 as ::core::ffi::c_int;
                }
                2 => {
                    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                    if !(in_grouping_b(
                        z,
                        &raw const g_aeo as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        111 as ::core::ffi::c_int,
                        0 as ::core::ffi::c_int,
                    ) != 0)
                    {
                        if !((*z).c > (*z).lb) {
                            return 0 as ::core::ffi::c_int;
                        }
                    }
                    (*z).c = (*z).l - m3;
                }
                3 => {
                    if (*z).c != *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
                        return 0 as ::core::ffi::c_int;
                    }
                    let mut m_test4: ::core::ffi::c_int = (*z).l - (*z).c;
                    let mut ret_4: ::core::ffi::c_int = r_shortv(z);
                    if ret_4 <= 0 as ::core::ffi::c_int {
                        return ret_4;
                    }
                    (*z).c = (*z).l - m_test4;
                    let mut ret_5: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_7 as *const symbol,
                    );
                    if ret_5 < 0 as ::core::ffi::c_int {
                        return ret_5;
                    }
                    return 0 as ::core::ffi::c_int;
                }
                _ => {}
            }
            (*z).c = (*z).l - m_test2;
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
            (*z).bra = (*z).c;
            let mut ret_6: ::core::ffi::c_int = slice_del(z);
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Step_1c(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'y' as i32
    {
        (*z).c = (*z).l - m1;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 'Y' as i32
        {
            return 0 as ::core::ffi::c_int;
        }
        (*z).c -= 1;
    } else {
        (*z).c -= 1;
    }
    (*z).bra = (*z).c;
    if out_grouping_b(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        121 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        let mut ret: ::core::ffi::c_int = slice_from_s(
            z,
            1 as ::core::ffi::c_int,
            &raw const s_8 as *const symbol,
        );
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
        return 1 as ::core::ffi::c_int;
    } else {
        return 0 as ::core::ffi::c_int
    };
}
unsafe fn r_Step_2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 815616 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_5 as *const among,
        24 as ::core::ffi::c_int,
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
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_9 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_10 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_11 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        4 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_12 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        5 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_13 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        6 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_14 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        7 => {
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_15 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        8 => {
            let mut ret_7: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_16 as *const symbol,
            );
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
        }
        9 => {
            let mut ret_8: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_17 as *const symbol,
            );
            if ret_8 < 0 as ::core::ffi::c_int {
                return ret_8;
            }
        }
        10 => {
            let mut ret_9: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_18 as *const symbol,
            );
            if ret_9 < 0 as ::core::ffi::c_int {
                return ret_9;
            }
        }
        11 => {
            let mut ret_10: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_19 as *const symbol,
            );
            if ret_10 < 0 as ::core::ffi::c_int {
                return ret_10;
            }
        }
        12 => {
            let mut ret_11: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_20 as *const symbol,
            );
            if ret_11 < 0 as ::core::ffi::c_int {
                return ret_11;
            }
        }
        13 => {
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'l' as i32
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
            let mut ret_12: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_21 as *const symbol,
            );
            if ret_12 < 0 as ::core::ffi::c_int {
                return ret_12;
            }
        }
        14 => {
            let mut ret_13: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_22 as *const symbol,
            );
            if ret_13 < 0 as ::core::ffi::c_int {
                return ret_13;
            }
        }
        15 => {
            if in_grouping_b(
                z,
                &raw const g_valid_LI as *const ::core::ffi::c_uchar,
                99 as ::core::ffi::c_int,
                116 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_14: ::core::ffi::c_int = slice_del(z);
            if ret_14 < 0 as ::core::ffi::c_int {
                return ret_14;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Step_3(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 528928 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_6 as *const among, 9 as ::core::ffi::c_int);
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
                4 as ::core::ffi::c_int,
                &raw const s_23 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_24 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_25 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        4 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_26 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        5 => {
            let mut ret_4: ::core::ffi::c_int = slice_del(z);
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        6 => {
            let mut ret_5: ::core::ffi::c_int = r_R2(z);
            if ret_5 <= 0 as ::core::ffi::c_int {
                return ret_5;
            }
            let mut ret_6: ::core::ffi::c_int = slice_del(z);
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Step_4(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 1864232 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_7 as *const among,
        18 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R2(z);
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
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 's' as i32
            {
                (*z).c = (*z).l - m1;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 't' as i32
                {
                    return 0 as ::core::ffi::c_int;
                }
                (*z).c -= 1;
            } else {
                (*z).c -= 1;
            }
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Step_5(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 101 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 108 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_8 as *const among, 2 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = r_R2(z);
            if ret == 0 as ::core::ffi::c_int {
                let mut ret_0: ::core::ffi::c_int = r_R1(z);
                if ret_0 <= 0 as ::core::ffi::c_int {
                    return ret_0;
                }
                let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_1: ::core::ffi::c_int = r_shortv(z);
                if ret_1 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m1;
                } else {
                    if ret_1 < 0 as ::core::ffi::c_int {
                        return ret_1;
                    }
                    return 0 as ::core::ffi::c_int;
                }
            } else if ret < 0 as ::core::ffi::c_int {
                return ret
            }
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        2 => {
            let mut ret_3: ::core::ffi::c_int = r_R2(z);
            if ret_3 <= 0 as ::core::ffi::c_int {
                return ret_3;
            }
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'l' as i32
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
            let mut ret_4: ::core::ffi::c_int = slice_del(z);
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_exception2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 5 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 100 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 103 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_9 as *const among, 8 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_exception1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    if (*z).c + 2 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 2 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 42750482 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c + 2 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among(z, &raw const a_10 as *const among, 18 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    if (*z).c < (*z).l {
        return 0 as ::core::ffi::c_int;
    }
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_27 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_28 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_29 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_30 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        5 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_31 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        6 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_32 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        7 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_33 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        8 => {
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_34 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        9 => {
            let mut ret_7: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_35 as *const symbol,
            );
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
        }
        10 => {
            let mut ret_8: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_36 as *const symbol,
            );
            if ret_8 < 0 as ::core::ffi::c_int {
                return ret_8;
            }
        }
        11 => {
            let mut ret_9: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_37 as *const symbol,
            );
            if ret_9 < 0 as ::core::ffi::c_int {
                return ret_9;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_postlude(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if *(*z).I.offset(2 as ::core::ffi::c_int as isize) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut current_block_11: u64;
    loop {
        let mut c1: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c2: ::core::ffi::c_int = (*z).c;
            (*z).bra = (*z).c;
            if (*z).c == (*z).l
                || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int != 'Y' as i32
            {
                (*z).c = c2;
                if (*z).c >= (*z).l {
                    current_block_11 = 9403074193669672085;
                    break;
                }
                (*z).c += 1;
            } else {
                (*z).c += 1;
                (*z).ket = (*z).c;
                (*z).c = c2;
                current_block_11 = 11650488183268122163;
                break;
            }
        }
        match current_block_11 {
            11650488183268122163 => {
                let mut ret: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_38 as *const symbol,
                );
                if ret < 0 as ::core::ffi::c_int {
                    return ret;
                }
            }
            _ => {
                (*z).c = c1;
                break;
            }
        }
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn english_ISO_8859_1_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_exception1(z);
    if ret == 0 as ::core::ffi::c_int {
        (*z).c = c1;
        let mut c2: ::core::ffi::c_int = (*z).c;
        (*z).c = (*z).c + 3 as ::core::ffi::c_int;
        if (*z).c > (*z).l {
            (*z).c = c2;
        } else {
            (*z).c = c1;
            let mut ret_0: ::core::ffi::c_int = r_prelude(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
            let mut ret_1: ::core::ffi::c_int = r_mark_regions(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
            (*z).lb = (*z).c;
            (*z).c = (*z).l;
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_2: ::core::ffi::c_int = r_Step_1a(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
            (*z).c = (*z).l - m3;
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_3: ::core::ffi::c_int = r_exception2(z);
            if ret_3 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m4;
                let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_4: ::core::ffi::c_int = r_Step_1b(z);
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
                (*z).c = (*z).l - m5;
                let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_5: ::core::ffi::c_int = r_Step_1c(z);
                if ret_5 < 0 as ::core::ffi::c_int {
                    return ret_5;
                }
                (*z).c = (*z).l - m6;
                let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_6: ::core::ffi::c_int = r_Step_2(z);
                if ret_6 < 0 as ::core::ffi::c_int {
                    return ret_6;
                }
                (*z).c = (*z).l - m7;
                let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_7: ::core::ffi::c_int = r_Step_3(z);
                if ret_7 < 0 as ::core::ffi::c_int {
                    return ret_7;
                }
                (*z).c = (*z).l - m8;
                let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_8: ::core::ffi::c_int = r_Step_4(z);
                if ret_8 < 0 as ::core::ffi::c_int {
                    return ret_8;
                }
                (*z).c = (*z).l - m9;
                let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_9: ::core::ffi::c_int = r_Step_5(z);
                if ret_9 < 0 as ::core::ffi::c_int {
                    return ret_9;
                }
                (*z).c = (*z).l - m10;
            } else if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3
            }
            (*z).c = (*z).lb;
            let mut c11: ::core::ffi::c_int = (*z).c;
            let mut ret_10: ::core::ffi::c_int = r_postlude(z);
            if ret_10 < 0 as ::core::ffi::c_int {
                return ret_10;
            }
            (*z).c = c11;
        }
    } else if ret < 0 as ::core::ffi::c_int {
        return ret
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn english_ISO_8859_1_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 3 as ::core::ffi::c_int);
}
pub unsafe fn english_ISO_8859_1_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
