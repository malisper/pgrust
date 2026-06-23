use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 1] = ['s' as i32 as symbol];
static mut s_0_1: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_2: [symbol; 4] = [
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_3: [symbol; 2] = ['s' as i32 as symbol, 's' as i32 as symbol];
static mut a_0: [among; 4] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_1_1: [symbol; 2] = ['b' as i32 as symbol, 'b' as i32 as symbol];
static mut s_1_2: [symbol; 2] = ['d' as i32 as symbol, 'd' as i32 as symbol];
static mut s_1_3: [symbol; 2] = ['f' as i32 as symbol, 'f' as i32 as symbol];
static mut s_1_4: [symbol; 2] = ['g' as i32 as symbol, 'g' as i32 as symbol];
static mut s_1_5: [symbol; 2] = ['b' as i32 as symbol, 'l' as i32 as symbol];
static mut s_1_6: [symbol; 2] = ['m' as i32 as symbol, 'm' as i32 as symbol];
static mut s_1_7: [symbol; 2] = ['n' as i32 as symbol, 'n' as i32 as symbol];
static mut s_1_8: [symbol; 2] = ['p' as i32 as symbol, 'p' as i32 as symbol];
static mut s_1_9: [symbol; 2] = ['r' as i32 as symbol, 'r' as i32 as symbol];
static mut s_1_10: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_1_11: [symbol; 2] = ['t' as i32 as symbol, 't' as i32 as symbol];
static mut s_1_12: [symbol; 2] = ['i' as i32 as symbol, 'z' as i32 as symbol];
static mut a_1: [among; 13] = unsafe {
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
            s: &raw const s_1_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_9 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_10 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_12 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 2] = ['e' as i32 as symbol, 'd' as i32 as symbol];
static mut s_2_1: [symbol; 3] = [
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_2_2: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut a_2: [among; 3] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_1: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_2: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_3: [symbol; 3] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_4: [symbol; 4] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_5: [symbol; 5] = [
    'o' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_6: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_7: [symbol; 5] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_8: [symbol; 6] = [
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_9: [symbol; 5] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_10: [symbol; 6] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_3_11: [symbol; 7] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_3_12: [symbol; 5] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_13: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_14: [symbol; 7] = [
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_15: [symbol; 4] = [
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_16: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_17: [symbol; 7] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_18: [symbol; 7] = [
    'f' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3_19: [symbol; 7] = [
    'o' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut a_3: [among; 20] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_14 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_1: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_2: [symbol; 5] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_3: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_4: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_4_5: [symbol; 3] = [
    'f' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_4_6: [symbol; 4] = [
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
];
static mut a_4: [among; 7] = unsafe {
    [
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_5_1: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_2: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_3: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_4: [symbol; 4] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_5: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_6: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_7: [symbol; 3] = [
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_8: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_9: [symbol; 2] = ['a' as i32 as symbol, 'l' as i32 as symbol];
static mut s_5_10: [symbol; 3] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_5_11: [symbol; 3] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_5_12: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_5_13: [symbol; 3] = [
    'o' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_14: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_15: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_16: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_17: [symbol; 5] = [
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_18: [symbol; 2] = ['o' as i32 as symbol, 'u' as i32 as symbol];
static mut a_5: [among; 19] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
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
static mut s_0: [symbol; 2] = ['s' as i32 as symbol, 's' as i32 as symbol];
static mut s_1: [symbol; 1] = ['i' as i32 as symbol];
static mut s_2: [symbol; 2] = ['e' as i32 as symbol, 'e' as i32 as symbol];
static mut s_3: [symbol; 1] = ['e' as i32 as symbol];
static mut s_4: [symbol; 1] = ['e' as i32 as symbol];
static mut s_5: [symbol; 1] = ['i' as i32 as symbol];
static mut s_6: [symbol; 4] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_7: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_8: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_9: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_10: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_11: [symbol; 1] = ['e' as i32 as symbol];
static mut s_12: [symbol; 3] = [
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_13: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_14: [symbol; 2] = ['a' as i32 as symbol, 'l' as i32 as symbol];
static mut s_15: [symbol; 3] = [
    'f' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_16: [symbol; 3] = [
    'o' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_17: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_18: [symbol; 3] = [
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_19: [symbol; 2] = ['a' as i32 as symbol, 'l' as i32 as symbol];
static mut s_20: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_21: [symbol; 1] = ['Y' as i32 as symbol];
static mut s_22: [symbol; 1] = ['Y' as i32 as symbol];
static mut s_23: [symbol; 1] = ['y' as i32 as symbol];
unsafe fn r_shortv(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if out_grouping_b(
        z,
        &raw const g_v_WXY as *const ::core::ffi::c_uchar,
        89 as ::core::ffi::c_int,
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
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 115 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_0 as *const among, 4 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
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
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
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
            as ::core::ffi::c_int != 100 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 103 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_2 as *const among, 3 as ::core::ffi::c_int);
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
                &raw const s_2 as *const symbol,
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
                    &raw const a_1 as *const among,
                    13 as ::core::ffi::c_int,
                );
            }
            (*z).c = (*z).l - m_test2;
            match among_var {
                1 => {
                    let mut ret_3: ::core::ffi::c_int = 0;
                    let mut saved_c: ::core::ffi::c_int = (*z).c;
                    ret_3 = insert_s(
                        z,
                        (*z).c,
                        (*z).c,
                        1 as ::core::ffi::c_int,
                        &raw const s_3 as *const symbol,
                    );
                    (*z).c = saved_c;
                    if ret_3 < 0 as ::core::ffi::c_int {
                        return ret_3;
                    }
                }
                2 => {
                    (*z).ket = (*z).c;
                    if (*z).c <= (*z).lb {
                        return 0 as ::core::ffi::c_int;
                    }
                    (*z).c -= 1;
                    (*z).bra = (*z).c;
                    let mut ret_4: ::core::ffi::c_int = slice_del(z);
                    if ret_4 < 0 as ::core::ffi::c_int {
                        return ret_4;
                    }
                }
                3 => {
                    if (*z).c != *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
                        return 0 as ::core::ffi::c_int;
                    }
                    let mut m_test3: ::core::ffi::c_int = (*z).l - (*z).c;
                    let mut ret_5: ::core::ffi::c_int = r_shortv(z);
                    if ret_5 <= 0 as ::core::ffi::c_int {
                        return ret_5;
                    }
                    (*z).c = (*z).l - m_test3;
                    let mut ret_6: ::core::ffi::c_int = 0;
                    let mut saved_c_0: ::core::ffi::c_int = (*z).c;
                    ret_6 = insert_s(
                        z,
                        (*z).c,
                        (*z).c,
                        1 as ::core::ffi::c_int,
                        &raw const s_4 as *const symbol,
                    );
                    (*z).c = saved_c_0;
                    if ret_6 < 0 as ::core::ffi::c_int {
                        return ret_6;
                    }
                }
                _ => {}
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
    let mut ret: ::core::ffi::c_int = out_grouping_b(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        121 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if ret < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= ret;
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        1 as ::core::ffi::c_int,
        &raw const s_5 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Step_2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
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
        &raw const a_3 as *const among,
        20 as ::core::ffi::c_int,
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
                &raw const s_6 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_8 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        4 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_9 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        5 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_10 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        6 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_11 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        7 => {
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_12 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        8 => {
            let mut ret_7: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_13 as *const symbol,
            );
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
        }
        9 => {
            let mut ret_8: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_14 as *const symbol,
            );
            if ret_8 < 0 as ::core::ffi::c_int {
                return ret_8;
            }
        }
        10 => {
            let mut ret_9: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_15 as *const symbol,
            );
            if ret_9 < 0 as ::core::ffi::c_int {
                return ret_9;
            }
        }
        11 => {
            let mut ret_10: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_16 as *const symbol,
            );
            if ret_10 < 0 as ::core::ffi::c_int {
                return ret_10;
            }
        }
        12 => {
            let mut ret_11: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_17 as *const symbol,
            );
            if ret_11 < 0 as ::core::ffi::c_int {
                return ret_11;
            }
        }
        13 => {
            let mut ret_12: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_18 as *const symbol,
            );
            if ret_12 < 0 as ::core::ffi::c_int {
                return ret_12;
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
    among_var = find_among_b(z, &raw const a_4 as *const among, 7 as ::core::ffi::c_int);
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
                2 as ::core::ffi::c_int,
                &raw const s_19 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_20 as *const symbol,
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
unsafe fn r_Step_4(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 3961384 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_5 as *const among,
        19 as ::core::ffi::c_int,
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
unsafe fn r_Step_5a(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'e' as i32
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= 1;
    (*z).bra = (*z).c;
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
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Step_5b(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'l' as i32
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= 1;
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R2(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'l' as i32
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= 1;
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn porter_ISO_8859_1_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut c1: ::core::ffi::c_int = (*z).c;
    (*z).bra = (*z).c;
    if !((*z).c == (*z).l
        || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int != 'y' as i32)
    {
        (*z).c += 1;
        (*z).ket = (*z).c;
        let mut ret: ::core::ffi::c_int = slice_from_s(
            z,
            1 as ::core::ffi::c_int,
            &raw const s_21 as *const symbol,
        );
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
        *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    }
    (*z).c = c1;
    let mut c2: ::core::ffi::c_int = (*z).c;
    loop {
        let mut c3: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c4: ::core::ffi::c_int = (*z).c;
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
                    (*z).c = c4;
                    current_block = 2838571290723028321;
                    break;
                }
            }
            (*z).c = c4;
            if (*z).c >= (*z).l {
                current_block = 4568606123710315785;
                break;
            }
            (*z).c += 1;
        }
        match current_block {
            4568606123710315785 => {
                (*z).c = c3;
                break;
            }
            _ => {
                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_22 as *const symbol,
                );
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
                *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 1
                    as ::core::ffi::c_int;
            }
        }
    }
    (*z).c = c2;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c5: ::core::ffi::c_int = (*z).c;
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
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
            let mut ret_3: ::core::ffi::c_int = out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                121 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_3 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_3;
                let mut ret_4: ::core::ffi::c_int = in_grouping(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    121 as ::core::ffi::c_int,
                    1 as ::core::ffi::c_int,
                );
                if !(ret_4 < 0 as ::core::ffi::c_int) {
                    (*z).c += ret_4;
                    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
                }
            }
        }
    }
    (*z).c = c5;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_5: ::core::ffi::c_int = r_Step_1a(z);
    if ret_5 < 0 as ::core::ffi::c_int {
        return ret_5;
    }
    (*z).c = (*z).l - m6;
    let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_6: ::core::ffi::c_int = r_Step_1b(z);
    if ret_6 < 0 as ::core::ffi::c_int {
        return ret_6;
    }
    (*z).c = (*z).l - m7;
    let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_7: ::core::ffi::c_int = r_Step_1c(z);
    if ret_7 < 0 as ::core::ffi::c_int {
        return ret_7;
    }
    (*z).c = (*z).l - m8;
    let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_8: ::core::ffi::c_int = r_Step_2(z);
    if ret_8 < 0 as ::core::ffi::c_int {
        return ret_8;
    }
    (*z).c = (*z).l - m9;
    let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_9: ::core::ffi::c_int = r_Step_3(z);
    if ret_9 < 0 as ::core::ffi::c_int {
        return ret_9;
    }
    (*z).c = (*z).l - m10;
    let mut m11: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_10: ::core::ffi::c_int = r_Step_4(z);
    if ret_10 < 0 as ::core::ffi::c_int {
        return ret_10;
    }
    (*z).c = (*z).l - m11;
    let mut m12: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_11: ::core::ffi::c_int = r_Step_5a(z);
    if ret_11 < 0 as ::core::ffi::c_int {
        return ret_11;
    }
    (*z).c = (*z).l - m12;
    let mut m13: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_12: ::core::ffi::c_int = r_Step_5b(z);
    if ret_12 < 0 as ::core::ffi::c_int {
        return ret_12;
    }
    (*z).c = (*z).l - m13;
    (*z).c = (*z).lb;
    let mut c14: ::core::ffi::c_int = (*z).c;
    if !(*(*z).I.offset(2 as ::core::ffi::c_int as isize) == 0) {
        let mut current_block_91: u64;
        loop {
            let mut c15: ::core::ffi::c_int = (*z).c;
            loop {
                let mut c16: ::core::ffi::c_int = (*z).c;
                (*z).bra = (*z).c;
                if (*z).c == (*z).l
                    || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                        != 'Y' as i32
                {
                    (*z).c = c16;
                    if (*z).c >= (*z).l {
                        current_block_91 = 1005300048611893664;
                        break;
                    }
                    (*z).c += 1;
                } else {
                    (*z).c += 1;
                    (*z).ket = (*z).c;
                    (*z).c = c16;
                    current_block_91 = 16313536926714486912;
                    break;
                }
            }
            match current_block_91 {
                16313536926714486912 => {
                    let mut ret_13: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_23 as *const symbol,
                    );
                    if ret_13 < 0 as ::core::ffi::c_int {
                        return ret_13;
                    }
                }
                _ => {
                    (*z).c = c15;
                    break;
                }
            }
        }
    }
    (*z).c = c14;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn porter_ISO_8859_1_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 3 as ::core::ffi::c_int);
}
pub unsafe fn porter_ISO_8859_1_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
