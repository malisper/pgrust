use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 2] = ['g' as i32 as symbol, 'i' as i32 as symbol];
static mut s_0_1: [symbol; 2] = ['k' as i32 as symbol, 'i' as i32 as symbol];
static mut a_0: [among; 2] = unsafe {
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
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 2] = ['d' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_1: [symbol; 4] = [
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_2: [symbol; 1] = ['b' as i32 as symbol];
static mut s_1_3: [symbol; 4] = [
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_1_4: [symbol; 6] = [
    'n' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_1_5: [symbol; 2] = ['m' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_6: [symbol; 4] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_7: [symbol; 5] = [
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_8: [symbol; 7] = [
    'n' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_9: [symbol; 4] = [
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_10: [symbol; 5] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_11: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_12: [symbol; 4] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_13: [symbol; 5] = [
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_14: [symbol; 7] = [
    'n' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_15: [symbol; 1] = ['n' as i32 as symbol];
static mut s_1_16: [symbol; 3] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_17: [symbol; 4] = [
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_18: [symbol; 6] = [
    'n' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_19: [symbol; 4] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_20: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut a_1: [among; 21] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_1_14 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 2] = ['a' as i32 as symbol, 'a' as i32 as symbol];
static mut s_2_1: [symbol; 2] = ['e' as i32 as symbol, 'e' as i32 as symbol];
static mut s_2_2: [symbol; 2] = ['i' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_3: [symbol; 2] = ['o' as i32 as symbol, 'o' as i32 as symbol];
static mut s_2_4: [symbol; 2] = ['u' as i32 as symbol, 'u' as i32 as symbol];
static mut s_2_5: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_2_6: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_2_7: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_2_8: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 9] = unsafe {
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 1] = ['i' as i32 as symbol];
static mut a_3: [among; 1] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_1: [symbol; 4] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_2: [symbol; 4] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_3: [symbol; 5] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_4: [symbol; 5] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_5: [symbol; 5] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_6: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_7: [symbol; 4] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_8: [symbol; 4] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_9: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_10: [symbol; 4] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_11: [symbol; 4] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
];
static mut a_4: [among; 12] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = ['g' as i32 as symbol, 'a' as i32 as symbol];
static mut s_5_1: [symbol; 2] = ['t' as i32 as symbol, 'a' as i32 as symbol];
static mut s_5_2: [symbol; 2] = ['l' as i32 as symbol, 'e' as i32 as symbol];
static mut s_5_3: [symbol; 3] = [
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_4: [symbol; 1] = ['l' as i32 as symbol];
static mut s_5_5: [symbol; 1] = ['s' as i32 as symbol];
static mut s_5_6: [symbol; 2] = ['k' as i32 as symbol, 's' as i32 as symbol];
static mut s_5_7: [symbol; 1] = ['t' as i32 as symbol];
static mut s_5_8: [symbol; 2] = ['l' as i32 as symbol, 't' as i32 as symbol];
static mut s_5_9: [symbol; 2] = ['s' as i32 as symbol, 't' as i32 as symbol];
static mut a_5: [among; 10] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_9 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_1: [symbol; 3] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_2: [symbol; 3] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_3: [symbol; 3] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_4: [symbol; 1] = ['t' as i32 as symbol];
static mut a_6: [among; 5] = unsafe {
    [
        among {
            s_size: 0 as ::core::ffi::c_int,
            s: ::core::ptr::null::<symbol>(),
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 1] = ['d' as i32 as symbol];
static mut s_7_1: [symbol; 3] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_7_2: [symbol; 2] = ['d' as i32 as symbol, 'e' as i32 as symbol];
static mut s_7_3: [symbol; 6] = [
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_4: [symbol; 3] = [
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_5: [symbol; 4] = [
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    'k' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_6: [symbol; 2] = ['t' as i32 as symbol, 'e' as i32 as symbol];
static mut a_7: [among; 7] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_8_0: [symbol; 2] = ['v' as i32 as symbol, 'a' as i32 as symbol];
static mut s_8_1: [symbol; 2] = ['d' as i32 as symbol, 'u' as i32 as symbol];
static mut s_8_2: [symbol; 2] = ['n' as i32 as symbol, 'u' as i32 as symbol];
static mut s_8_3: [symbol; 2] = ['t' as i32 as symbol, 'u' as i32 as symbol];
static mut a_8: [among; 4] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_9_0: [symbol; 2] = ['k' as i32 as symbol, 'k' as i32 as symbol];
static mut s_9_1: [symbol; 2] = ['p' as i32 as symbol, 'p' as i32 as symbol];
static mut s_9_2: [symbol; 2] = ['t' as i32 as symbol, 't' as i32 as symbol];
static mut a_9: [among; 3] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_10_0: [symbol; 2] = ['m' as i32 as symbol, 'a' as i32 as symbol];
static mut s_10_1: [symbol; 3] = [
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_10_2: [symbol; 1] = ['m' as i32 as symbol];
static mut a_10: [among; 3] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
    ]
};
static mut s_11_0: [symbol; 4] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_1: [symbol; 4] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_2: [symbol; 8] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_3: [symbol; 5] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_4: [symbol; 7] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_5: [symbol; 5] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_6: [symbol; 4] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_7: [symbol; 5] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_8: [symbol; 6] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_9: [symbol; 4] = [
    'j' as i32 as symbol,
    'u' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_10: [symbol; 7] = [
    'j' as i32 as symbol,
    'u' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_11: [symbol; 4] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_11_12: [symbol; 5] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_13: [symbol; 6] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_14: [symbol; 5] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_15: [symbol; 6] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_16: [symbol; 6] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'b' as i32 as symbol,
];
static mut s_11_17: [symbol; 6] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
];
static mut s_11_18: [symbol; 7] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_19: [symbol; 10] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_20: [symbol; 7] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_21: [symbol; 7] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_22: [symbol; 9] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_23: [symbol; 10] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_24: [symbol; 9] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_25: [symbol; 10] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_26: [symbol; 7] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_27: [symbol; 9] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_28: [symbol; 7] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_29: [symbol; 6] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_11_30: [symbol; 7] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_31: [symbol; 8] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_32: [symbol; 4] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_11_33: [symbol; 5] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_34: [symbol; 6] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_35: [symbol; 5] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_36: [symbol; 6] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_37: [symbol; 4] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_38: [symbol; 4] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_39: [symbol; 8] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_40: [symbol; 5] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_41: [symbol; 7] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_42: [symbol; 8] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_43: [symbol; 7] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_44: [symbol; 8] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_45: [symbol; 5] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_46: [symbol; 7] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_47: [symbol; 5] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_48: [symbol; 4] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_49: [symbol; 4] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_50: [symbol; 5] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_51: [symbol; 5] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_52: [symbol; 6] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_53: [symbol; 5] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_54: [symbol; 8] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_55: [symbol; 5] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_56: [symbol; 5] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_57: [symbol; 6] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_58: [symbol; 6] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_59: [symbol; 8] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_60: [symbol; 9] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_61: [symbol; 8] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_62: [symbol; 9] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_63: [symbol; 6] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_64: [symbol; 8] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_65: [symbol; 6] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_66: [symbol; 5] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_67: [symbol; 5] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_68: [symbol; 6] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_69: [symbol; 7] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_70: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_71: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_72: [symbol; 5] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_73: [symbol; 7] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_74: [symbol; 8] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_75: [symbol; 7] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_76: [symbol; 8] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_77: [symbol; 5] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_78: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_79: [symbol; 5] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_80: [symbol; 6] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'o' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_81: [symbol; 4] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_82: [symbol; 4] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_83: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_84: [symbol; 7] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_85: [symbol; 8] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_86: [symbol; 7] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_87: [symbol; 8] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_88: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_89: [symbol; 4] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_90: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_91: [symbol; 6] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_92: [symbol; 4] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_93: [symbol; 4] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_94: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_95: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_96: [symbol; 7] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_97: [symbol; 8] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_98: [symbol; 7] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_99: [symbol; 8] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_100: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_101: [symbol; 7] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_102: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_103: [symbol; 4] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_104: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_105: [symbol; 6] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_106: [symbol; 4] = [
    'l' as i32 as symbol,
    'u' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_107: [symbol; 7] = [
    'l' as i32 as symbol,
    'u' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_108: [symbol; 4] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_11_109: [symbol; 5] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_110: [symbol; 6] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_111: [symbol; 5] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_112: [symbol; 6] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_113: [symbol; 6] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'b' as i32 as symbol,
];
static mut s_11_114: [symbol; 6] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
];
static mut s_11_115: [symbol; 10] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_116: [symbol; 7] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_117: [symbol; 7] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_118: [symbol; 9] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_119: [symbol; 10] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_120: [symbol; 9] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_121: [symbol; 10] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_122: [symbol; 7] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_123: [symbol; 9] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_124: [symbol; 7] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_125: [symbol; 6] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_11_126: [symbol; 7] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_127: [symbol; 8] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_128: [symbol; 6] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_11_129: [symbol; 9] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_130: [symbol; 6] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_11_131: [symbol; 9] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_132: [symbol; 6] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'b' as i32 as symbol,
];
static mut s_11_133: [symbol; 6] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
];
static mut s_11_134: [symbol; 7] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_135: [symbol; 7] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_136: [symbol; 9] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_137: [symbol; 10] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_138: [symbol; 9] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_139: [symbol; 10] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_140: [symbol; 7] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_141: [symbol; 9] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_142: [symbol; 7] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_143: [symbol; 6] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_11_144: [symbol; 6] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_11_145: [symbol; 7] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_146: [symbol; 8] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_147: [symbol; 5] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_148: [symbol; 5] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_149: [symbol; 6] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_150: [symbol; 8] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_151: [symbol; 9] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_152: [symbol; 8] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_153: [symbol; 9] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_154: [symbol; 6] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_155: [symbol; 5] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_156: [symbol; 6] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_157: [symbol; 7] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_158: [symbol; 7] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_159: [symbol; 9] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_160: [symbol; 5] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'h' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_161: [symbol; 8] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'h' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_162: [symbol; 6] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_163: [symbol; 5] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_164: [symbol; 5] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_165: [symbol; 6] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_166: [symbol; 8] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_167: [symbol; 9] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_168: [symbol; 8] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_169: [symbol; 9] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_170: [symbol; 6] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_171: [symbol; 5] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_172: [symbol; 6] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_173: [symbol; 7] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_174: [symbol; 4] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_175: [symbol; 4] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_176: [symbol; 5] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_177: [symbol; 8] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_178: [symbol; 5] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_179: [symbol; 5] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_180: [symbol; 7] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_181: [symbol; 8] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_182: [symbol; 7] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_183: [symbol; 8] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_184: [symbol; 5] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_185: [symbol; 7] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_186: [symbol; 5] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_187: [symbol; 4] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_188: [symbol; 5] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_189: [symbol; 6] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_190: [symbol; 3] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_191: [symbol; 4] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_192: [symbol; 5] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_193: [symbol; 4] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_194: [symbol; 5] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_195: [symbol; 4] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_11_196: [symbol; 5] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_197: [symbol; 6] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_198: [symbol; 5] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_199: [symbol; 6] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_200: [symbol; 6] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'b' as i32 as symbol,
];
static mut s_11_201: [symbol; 6] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
];
static mut s_11_202: [symbol; 10] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_203: [symbol; 7] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_204: [symbol; 7] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_205: [symbol; 9] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_206: [symbol; 10] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_207: [symbol; 9] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_208: [symbol; 10] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_209: [symbol; 7] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_210: [symbol; 9] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_211: [symbol; 7] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_212: [symbol; 6] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_11_213: [symbol; 7] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_214: [symbol; 8] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_215: [symbol; 6] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_11_216: [symbol; 9] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_217: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_218: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_219: [symbol; 5] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_220: [symbol; 7] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_221: [symbol; 8] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_222: [symbol; 7] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_223: [symbol; 8] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_224: [symbol; 5] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_225: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_226: [symbol; 5] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_227: [symbol; 6] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_228: [symbol; 6] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_229: [symbol; 8] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_230: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'h' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_231: [symbol; 7] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'h' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_232: [symbol; 5] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'h' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_233: [symbol; 4] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_234: [symbol; 4] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_235: [symbol; 5] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_236: [symbol; 5] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_237: [symbol; 7] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_238: [symbol; 8] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_239: [symbol; 7] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_240: [symbol; 8] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_241: [symbol; 5] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_242: [symbol; 7] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_243: [symbol; 5] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_244: [symbol; 4] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_245: [symbol; 5] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_246: [symbol; 6] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_247: [symbol; 4] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_248: [symbol; 7] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_249: [symbol; 4] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_11_250: [symbol; 5] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_251: [symbol; 6] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_252: [symbol; 5] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_253: [symbol; 6] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_254: [symbol; 4] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_255: [symbol; 7] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_256: [symbol; 4] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_257: [symbol; 4] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_258: [symbol; 5] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_259: [symbol; 5] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_260: [symbol; 7] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_261: [symbol; 8] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_262: [symbol; 7] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_263: [symbol; 8] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_264: [symbol; 5] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_265: [symbol; 7] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_266: [symbol; 5] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_267: [symbol; 4] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_268: [symbol; 7] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_269: [symbol; 6] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_270: [symbol; 7] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_271: [symbol; 5] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_272: [symbol; 6] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_273: [symbol; 5] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'b' as i32 as symbol,
];
static mut s_11_274: [symbol; 5] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_275: [symbol; 6] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_276: [symbol; 9] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_277: [symbol; 6] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_11_278: [symbol; 6] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_279: [symbol; 8] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_11_280: [symbol; 9] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_281: [symbol; 8] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_282: [symbol; 9] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_283: [symbol; 6] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_284: [symbol; 8] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_11_285: [symbol; 6] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_286: [symbol; 5] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_11_287: [symbol; 5] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_11_288: [symbol; 6] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_11_289: [symbol; 7] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut a_11: [among; 290] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_12 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_13 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_14 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_15 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_20 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_22 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_23 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_24 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_25 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_33 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_34 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_35 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_36 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_39 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_41 as *const symbol,
            substring_i: 40 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_42 as *const symbol,
            substring_i: 40 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_43 as *const symbol,
            substring_i: 40 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_44 as *const symbol,
            substring_i: 40 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_46 as *const symbol,
            substring_i: 45 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_47 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_50 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_51 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_53 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_54 as *const symbol,
            substring_i: 53 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_55 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_56 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_57 as *const symbol,
            substring_i: 56 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_58 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_59 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_60 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_61 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_62 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_63 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_64 as *const symbol,
            substring_i: 63 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_65 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_66 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_67 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_68 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_69 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_70 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_71 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_72 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_73 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_74 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_75 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_76 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_77 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_78 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_79 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_80 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_81 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_82 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_83 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_84 as *const symbol,
            substring_i: 83 as ::core::ffi::c_int,
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_85 as *const symbol,
            substring_i: 83 as ::core::ffi::c_int,
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_86 as *const symbol,
            substring_i: 83 as ::core::ffi::c_int,
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_87 as *const symbol,
            substring_i: 83 as ::core::ffi::c_int,
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_88 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_89 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_90 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_91 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_92 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_93 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_94 as *const symbol,
            substring_i: 93 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_95 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_96 as *const symbol,
            substring_i: 95 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_97 as *const symbol,
            substring_i: 95 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_98 as *const symbol,
            substring_i: 95 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_99 as *const symbol,
            substring_i: 95 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_100 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_101 as *const symbol,
            substring_i: 100 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_102 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_103 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_104 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_105 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_106 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_107 as *const symbol,
            substring_i: 106 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_108 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_109 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_110 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_111 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_112 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_113 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_114 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_115 as *const symbol,
            substring_i: 114 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_116 as *const symbol,
            substring_i: 114 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_117 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_118 as *const symbol,
            substring_i: 117 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_119 as *const symbol,
            substring_i: 117 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_120 as *const symbol,
            substring_i: 117 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_121 as *const symbol,
            substring_i: 117 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_122 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_123 as *const symbol,
            substring_i: 122 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_124 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_125 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_126 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_127 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_128 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_129 as *const symbol,
            substring_i: 128 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_130 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_131 as *const symbol,
            substring_i: 130 as ::core::ffi::c_int,
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_132 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_133 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_134 as *const symbol,
            substring_i: 133 as ::core::ffi::c_int,
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_135 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_136 as *const symbol,
            substring_i: 135 as ::core::ffi::c_int,
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_137 as *const symbol,
            substring_i: 135 as ::core::ffi::c_int,
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_138 as *const symbol,
            substring_i: 135 as ::core::ffi::c_int,
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_139 as *const symbol,
            substring_i: 135 as ::core::ffi::c_int,
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_140 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_141 as *const symbol,
            substring_i: 140 as ::core::ffi::c_int,
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_142 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_143 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_144 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_145 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_146 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_147 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_148 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_149 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_150 as *const symbol,
            substring_i: 149 as ::core::ffi::c_int,
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_151 as *const symbol,
            substring_i: 149 as ::core::ffi::c_int,
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_152 as *const symbol,
            substring_i: 149 as ::core::ffi::c_int,
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_153 as *const symbol,
            substring_i: 149 as ::core::ffi::c_int,
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_154 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_155 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_156 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_157 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_158 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_159 as *const symbol,
            substring_i: 158 as ::core::ffi::c_int,
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_160 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_161 as *const symbol,
            substring_i: 160 as ::core::ffi::c_int,
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_162 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_163 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_164 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_165 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_166 as *const symbol,
            substring_i: 165 as ::core::ffi::c_int,
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_167 as *const symbol,
            substring_i: 165 as ::core::ffi::c_int,
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_168 as *const symbol,
            substring_i: 165 as ::core::ffi::c_int,
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_169 as *const symbol,
            substring_i: 165 as ::core::ffi::c_int,
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_170 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_171 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_172 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_173 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_174 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_175 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_176 as *const symbol,
            substring_i: 175 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_177 as *const symbol,
            substring_i: 176 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_178 as *const symbol,
            substring_i: 175 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_179 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_180 as *const symbol,
            substring_i: 179 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_181 as *const symbol,
            substring_i: 179 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_182 as *const symbol,
            substring_i: 179 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_183 as *const symbol,
            substring_i: 179 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_184 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_185 as *const symbol,
            substring_i: 184 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_186 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_187 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_188 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_189 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_190 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_191 as *const symbol,
            substring_i: 190 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_192 as *const symbol,
            substring_i: 190 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_193 as *const symbol,
            substring_i: 190 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_194 as *const symbol,
            substring_i: 190 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_195 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_196 as *const symbol,
            substring_i: 195 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_197 as *const symbol,
            substring_i: 195 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_198 as *const symbol,
            substring_i: 195 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_199 as *const symbol,
            substring_i: 195 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_200 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_201 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_202 as *const symbol,
            substring_i: 201 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_203 as *const symbol,
            substring_i: 201 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_204 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_205 as *const symbol,
            substring_i: 204 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_206 as *const symbol,
            substring_i: 204 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_207 as *const symbol,
            substring_i: 204 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_208 as *const symbol,
            substring_i: 204 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_209 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_210 as *const symbol,
            substring_i: 209 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_211 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_212 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_213 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_214 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_215 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_216 as *const symbol,
            substring_i: 215 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_217 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_218 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_219 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_220 as *const symbol,
            substring_i: 219 as ::core::ffi::c_int,
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_221 as *const symbol,
            substring_i: 219 as ::core::ffi::c_int,
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_222 as *const symbol,
            substring_i: 219 as ::core::ffi::c_int,
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_223 as *const symbol,
            substring_i: 219 as ::core::ffi::c_int,
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_224 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_225 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_226 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_227 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_228 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_229 as *const symbol,
            substring_i: 228 as ::core::ffi::c_int,
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_230 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_231 as *const symbol,
            substring_i: 230 as ::core::ffi::c_int,
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_232 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_233 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_234 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_235 as *const symbol,
            substring_i: 234 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_236 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_237 as *const symbol,
            substring_i: 236 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_238 as *const symbol,
            substring_i: 236 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_239 as *const symbol,
            substring_i: 236 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_240 as *const symbol,
            substring_i: 236 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_241 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_242 as *const symbol,
            substring_i: 241 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_243 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_244 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_245 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_246 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_247 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_248 as *const symbol,
            substring_i: 247 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_249 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_250 as *const symbol,
            substring_i: 249 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_251 as *const symbol,
            substring_i: 249 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_252 as *const symbol,
            substring_i: 249 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_253 as *const symbol,
            substring_i: 249 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_254 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_255 as *const symbol,
            substring_i: 254 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_256 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_257 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_258 as *const symbol,
            substring_i: 257 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_259 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_260 as *const symbol,
            substring_i: 259 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_261 as *const symbol,
            substring_i: 259 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_262 as *const symbol,
            substring_i: 259 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_263 as *const symbol,
            substring_i: 259 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_264 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_265 as *const symbol,
            substring_i: 264 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_266 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_11_267 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_268 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_269 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_270 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_271 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_272 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_273 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_274 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_275 as *const symbol,
            substring_i: 274 as ::core::ffi::c_int,
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_276 as *const symbol,
            substring_i: 275 as ::core::ffi::c_int,
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_277 as *const symbol,
            substring_i: 274 as ::core::ffi::c_int,
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_278 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_279 as *const symbol,
            substring_i: 278 as ::core::ffi::c_int,
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_280 as *const symbol,
            substring_i: 278 as ::core::ffi::c_int,
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_281 as *const symbol,
            substring_i: 278 as ::core::ffi::c_int,
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_11_282 as *const symbol,
            substring_i: 278 as ::core::ffi::c_int,
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_283 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_284 as *const symbol,
            substring_i: 283 as ::core::ffi::c_int,
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_285 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_286 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_11_287 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_11_288 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_11_289 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_V1: [::core::ffi::c_uchar; 20] = [
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
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    48 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_RV: [::core::ffi::c_uchar; 3] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_KI: [::core::ffi::c_uchar; 36] = [
    117 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    66 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    6 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    128 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_GI: [::core::ffi::c_uchar; 20] = [
    21 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    123 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    243 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    48 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_1: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2: [symbol; 4] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3: [symbol; 4] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4: [symbol; 3] = [
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_5: [symbol; 1] = ['e' as i32 as symbol];
static mut s_6: [symbol; 1] = ['t' as i32 as symbol];
static mut s_7: [symbol; 1] = ['k' as i32 as symbol];
static mut s_8: [symbol; 1] = ['p' as i32 as symbol];
static mut s_9: [symbol; 1] = ['t' as i32 as symbol];
static mut s_10: [symbol; 3] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_11: [symbol; 3] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_12: [symbol; 5] = [
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_13: [symbol; 5] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_14: [symbol; 5] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_15: [symbol; 4] = [
    'l' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_16: [symbol; 3] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_17: [symbol; 6] = [
    'k' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_18: [symbol; 5] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_19: [symbol; 3] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_20: [symbol; 6] = [
    'v' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_21: [symbol; 7] = [
    'j' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_22: [symbol; 7] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_23: [symbol; 4] = [
    'l' as i32 as symbol,
    'u' as i32 as symbol,
    'g' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_24: [symbol; 5] = [
    'p' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_25: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_26: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_27: [symbol; 5] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
];
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    if out_grouping_U(
        z,
        &raw const g_V1 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        252 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    ) < 0 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = in_grouping_U(
        z,
        &raw const g_V1 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        252 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if ret < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c += ret;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_emphasis(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 105 as ::core::ffi::c_int
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_0 as *const among, 2 as ::core::ffi::c_int);
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret: ::core::ffi::c_int = skip_b_utf8(
        (*z).p,
        (*z).c,
        (*z).lb,
        4 as ::core::ffi::c_int,
    );
    if ret < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c = ret;
    (*z).c = (*z).l - m_test2;
    match among_var {
        1 => {
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            if in_grouping_b_U(
                z,
                &raw const g_GI as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                252 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = (*z).l - m3;
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_0: ::core::ffi::c_int = r_LONGV(z);
            if ret_0 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m4;
            } else {
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        2 => {
            if in_grouping_b_U(
                z,
                &raw const g_KI as *const ::core::ffi::c_uchar,
                98 as ::core::ffi::c_int,
                382 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
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
unsafe fn r_verb(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 540726 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_1 as *const among,
        21 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
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
            if in_grouping_b_U(
                z,
                &raw const g_V1 as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                252 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
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
unsafe fn r_LONGV(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if find_among_b(z, &raw const a_2 as *const among, 9 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_i_plural(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 105 as ::core::ffi::c_int
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_3 as *const among, 1 as ::core::ffi::c_int) == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    if in_grouping_b_U(
        z,
        &raw const g_RV as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        117 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_special_noun_endings(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 1049120 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_4 as *const among,
        12 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_1 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_2 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
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
unsafe fn r_case_ending(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 1576994 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_5 as *const among,
        10 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    match among_var {
        1 => {
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            if in_grouping_b_U(
                z,
                &raw const g_RV as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                117 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                (*z).c = (*z).l - m2;
                let mut ret: ::core::ffi::c_int = r_LONGV(z);
                if ret <= 0 as ::core::ffi::c_int {
                    return ret;
                }
            }
        }
        2 => {
            let mut m_test3: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_0: ::core::ffi::c_int = skip_b_utf8(
                (*z).p,
                (*z).c,
                (*z).lb,
                4 as ::core::ffi::c_int,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = ret_0;
            (*z).c = (*z).l - m_test3;
        }
        _ => {}
    }
    let mut ret_1: ::core::ffi::c_int = slice_del(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_plural_three_first_cases(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 100 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_7 as *const among, 7 as ::core::ffi::c_int);
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_4 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_0: ::core::ffi::c_int = r_LONGV(z);
            if ret_0 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m2;
            } else {
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m_test4: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_2: ::core::ffi::c_int = skip_b_utf8(
                (*z).p,
                (*z).c,
                (*z).lb,
                4 as ::core::ffi::c_int,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m3;
                let mut ret_5: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_6 as *const symbol,
                );
                if ret_5 < 0 as ::core::ffi::c_int {
                    return ret_5;
                }
            } else {
                (*z).c = ret_2;
                (*z).c = (*z).l - m_test4;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 115 as ::core::ffi::c_int
                        && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                            as ::core::ffi::c_int != 116 as ::core::ffi::c_int
                {
                    among_var = 2 as ::core::ffi::c_int;
                } else {
                    among_var = find_among_b(
                        z,
                        &raw const a_6 as *const among,
                        5 as ::core::ffi::c_int,
                    );
                }
                match among_var {
                    1 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            1 as ::core::ffi::c_int,
                            &raw const s_5 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                    }
                    2 => {
                        let mut ret_4: ::core::ffi::c_int = slice_del(z);
                        if ret_4 < 0 as ::core::ffi::c_int {
                            return ret_4;
                        }
                    }
                    _ => {}
                }
            }
        }
        4 => {
            let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
            if in_grouping_b_U(
                z,
                &raw const g_RV as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                117 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                (*z).c = (*z).l - m5;
                let mut ret_6: ::core::ffi::c_int = r_LONGV(z);
                if ret_6 <= 0 as ::core::ffi::c_int {
                    return ret_6;
                }
            }
            let mut ret_7: ::core::ffi::c_int = slice_del(z);
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_nu(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 117 as ::core::ffi::c_int
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_8 as *const among, 4 as ::core::ffi::c_int) == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_undouble_kpt(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    if in_grouping_b_U(
        z,
        &raw const g_V1 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        252 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if *(*z).I.offset(0 as ::core::ffi::c_int as isize) > (*z).c {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 1116160 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_9 as *const among, 3 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_8 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_9 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_degrees(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 8706 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_10 as *const among,
        3 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    match among_var {
        1 => {
            if in_grouping_b_U(
                z,
                &raw const g_RV as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                117 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_substantive(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret: ::core::ffi::c_int = r_special_noun_endings(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = (*z).l - m1;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_case_ending(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_plural_three_first_cases(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m3;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_degrees(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m4;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_i_plural(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = (*z).l - m5;
    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_4: ::core::ffi::c_int = r_nu(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    (*z).c = (*z).l - m6;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_verb_exceptions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    among_var = find_among(
        z,
        &raw const a_11 as *const among,
        290 as ::core::ffi::c_int,
    );
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
                &raw const s_10 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_11 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_12 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_13 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        5 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_14 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        6 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_15 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        7 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_16 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        8 => {
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_17 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        9 => {
            let mut ret_7: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_18 as *const symbol,
            );
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
        }
        10 => {
            let mut ret_8: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_19 as *const symbol,
            );
            if ret_8 < 0 as ::core::ffi::c_int {
                return ret_8;
            }
        }
        11 => {
            let mut ret_9: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_20 as *const symbol,
            );
            if ret_9 < 0 as ::core::ffi::c_int {
                return ret_9;
            }
        }
        12 => {
            let mut ret_10: ::core::ffi::c_int = slice_from_s(
                z,
                7 as ::core::ffi::c_int,
                &raw const s_21 as *const symbol,
            );
            if ret_10 < 0 as ::core::ffi::c_int {
                return ret_10;
            }
        }
        13 => {
            let mut ret_11: ::core::ffi::c_int = slice_from_s(
                z,
                7 as ::core::ffi::c_int,
                &raw const s_22 as *const symbol,
            );
            if ret_11 < 0 as ::core::ffi::c_int {
                return ret_11;
            }
        }
        14 => {
            let mut ret_12: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_23 as *const symbol,
            );
            if ret_12 < 0 as ::core::ffi::c_int {
                return ret_12;
            }
        }
        15 => {
            let mut ret_13: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_24 as *const symbol,
            );
            if ret_13 < 0 as ::core::ffi::c_int {
                return ret_13;
            }
        }
        16 => {
            let mut ret_14: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_25 as *const symbol,
            );
            if ret_14 < 0 as ::core::ffi::c_int {
                return ret_14;
            }
        }
        17 => {
            let mut ret_15: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_26 as *const symbol,
            );
            if ret_15 < 0 as ::core::ffi::c_int {
                return ret_15;
            }
        }
        18 => {
            let mut ret_16: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_27 as *const symbol,
            );
            if ret_16 < 0 as ::core::ffi::c_int {
                return ret_16;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn estonian_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_verb_exceptions(z);
    if ret == 0 as ::core::ffi::c_int {
        (*z).c = c1;
    } else {
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
        return 0 as ::core::ffi::c_int;
    }
    let mut c2: ::core::ffi::c_int = (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_mark_regions(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = c2;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_emphasis(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m3;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_verb(z);
    if ret_2 == 0 as ::core::ffi::c_int {
        (*z).c = (*z).l - m5;
        let mut ret_3: ::core::ffi::c_int = r_substantive(z);
        if ret_3 < 0 as ::core::ffi::c_int {
            return ret_3;
        }
    } else if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2
    }
    (*z).c = (*z).l - m4;
    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_4: ::core::ffi::c_int = r_undouble_kpt(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    (*z).c = (*z).l - m6;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn estonian_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 1 as ::core::ffi::c_int);
}
pub unsafe fn estonian_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
