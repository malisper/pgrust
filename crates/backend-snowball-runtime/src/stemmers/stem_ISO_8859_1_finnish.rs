use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 2] = ['p' as i32 as symbol, 'a' as i32 as symbol];
static mut s_0_1: [symbol; 3] = [
    's' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_2: [symbol; 4] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_3: [symbol; 3] = [
    'h' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_4: [symbol; 3] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_5: [symbol; 3] = [
    'h' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_0_6: [symbol; 4] = [
    'k' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_0_7: [symbol; 2] = ['k' as i32 as symbol, 'o' as i32 as symbol];
static mut s_0_8: [symbol; 2] = [
    'p' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_0_9: [symbol; 2] = [
    'k' as i32 as symbol,
    0xf6 as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 10] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 3] = [
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_1: [symbol; 2] = ['n' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_2: [symbol; 3] = [
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_3: [symbol; 2] = ['t' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_4: [symbol; 3] = [
    'l' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_5: [symbol; 3] = [
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut a_1: [among; 6] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 3] = [
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_2_1: [symbol; 2] = [
    'n' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_2_2: [symbol; 3] = [
    's' as i32 as symbol,
    's' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_2_3: [symbol; 2] = [
    't' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_2_4: [symbol; 3] = [
    'l' as i32 as symbol,
    't' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_2_5: [symbol; 3] = [
    's' as i32 as symbol,
    't' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 6] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 3] = [
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_1: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut a_3: [among; 2] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 3] = [
    'n' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_1: [symbol; 3] = [
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_2: [symbol; 3] = [
    'n' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_3: [symbol; 2] = ['n' as i32 as symbol, 'i' as i32 as symbol];
static mut s_4_4: [symbol; 2] = ['s' as i32 as symbol, 'i' as i32 as symbol];
static mut s_4_5: [symbol; 2] = ['a' as i32 as symbol, 'n' as i32 as symbol];
static mut s_4_6: [symbol; 2] = ['e' as i32 as symbol, 'n' as i32 as symbol];
static mut s_4_7: [symbol; 2] = [
    0xe4 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_4_8: [symbol; 3] = [
    'n' as i32 as symbol,
    's' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 9] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = ['a' as i32 as symbol, 'a' as i32 as symbol];
static mut s_5_1: [symbol; 2] = ['e' as i32 as symbol, 'e' as i32 as symbol];
static mut s_5_2: [symbol; 2] = ['i' as i32 as symbol, 'i' as i32 as symbol];
static mut s_5_3: [symbol; 2] = ['o' as i32 as symbol, 'o' as i32 as symbol];
static mut s_5_4: [symbol; 2] = ['u' as i32 as symbol, 'u' as i32 as symbol];
static mut s_5_5: [symbol; 2] = [
    0xe4 as ::core::ffi::c_int as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_5_6: [symbol; 2] = [
    0xf6 as ::core::ffi::c_int as symbol,
    0xf6 as ::core::ffi::c_int as symbol,
];
static mut a_5: [among; 7] = unsafe {
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
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_6_1: [symbol; 3] = [
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_2: [symbol; 2] = ['n' as i32 as symbol, 'a' as i32 as symbol];
static mut s_6_3: [symbol; 3] = [
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_4: [symbol; 2] = ['t' as i32 as symbol, 'a' as i32 as symbol];
static mut s_6_5: [symbol; 3] = [
    'l' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_6: [symbol; 3] = [
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_7: [symbol; 3] = [
    't' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_8: [symbol; 3] = [
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_9: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_10: [symbol; 3] = [
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_11: [symbol; 1] = ['n' as i32 as symbol];
static mut s_6_12: [symbol; 3] = [
    'h' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_6_13: [symbol; 3] = [
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_6_14: [symbol; 4] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_6_15: [symbol; 3] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_6_16: [symbol; 4] = [
    't' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_6_17: [symbol; 3] = [
    'h' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_6_18: [symbol; 4] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_6_19: [symbol; 3] = [
    'h' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_6_20: [symbol; 3] = [
    'h' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_6_21: [symbol; 3] = [
    'h' as i32 as symbol,
    0xf6 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_6_22: [symbol; 1] = [0xe4 as ::core::ffi::c_int as symbol];
static mut s_6_23: [symbol; 3] = [
    'l' as i32 as symbol,
    'l' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_6_24: [symbol; 2] = [
    'n' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_6_25: [symbol; 3] = [
    's' as i32 as symbol,
    's' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_6_26: [symbol; 2] = [
    't' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_6_27: [symbol; 3] = [
    'l' as i32 as symbol,
    't' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_6_28: [symbol; 3] = [
    's' as i32 as symbol,
    't' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_6_29: [symbol; 3] = [
    't' as i32 as symbol,
    't' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut a_6: [among; 30] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_6 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_7 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_6_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_12 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_13 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: Some(
                r_VI as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_14 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: Some(
                r_LONG as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_15 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_16 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: Some(
                r_VI as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_17 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_18 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: Some(
                r_VI as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_19 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_20 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_21 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_6_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_24 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_25 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_26 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_28 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_29 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 3] = [
    'e' as i32 as symbol,
    'j' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_1: [symbol; 3] = [
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_2: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_3: [symbol; 3] = [
    'm' as i32 as symbol,
    'p' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_4: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'p' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_5: [symbol; 3] = [
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_6: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_7: [symbol; 3] = [
    'm' as i32 as symbol,
    'p' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_8: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'p' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_9: [symbol; 3] = [
    'e' as i32 as symbol,
    'j' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_7_10: [symbol; 3] = [
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_7_11: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_7_12: [symbol; 3] = [
    'm' as i32 as symbol,
    'p' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut s_7_13: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'p' as i32 as symbol,
    0xe4 as ::core::ffi::c_int as symbol,
];
static mut a_7: [among; 14] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_8_0: [symbol; 1] = ['i' as i32 as symbol];
static mut s_8_1: [symbol; 1] = ['j' as i32 as symbol];
static mut a_8: [among; 2] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_8_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_8_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_9_0: [symbol; 3] = [
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_9_1: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut a_9: [among; 2] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_9_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut g_AEI: [::core::ffi::c_uchar; 17] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_C: [::core::ffi::c_uchar; 4] = [
    119 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    223 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    119 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_V1: [::core::ffi::c_uchar; 19] = [
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
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    32 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_V2: [::core::ffi::c_uchar; 19] = [
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
    32 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_particle_end: [::core::ffi::c_uchar; 19] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    97 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    24 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    32 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 3] = [
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1: [symbol; 3] = [
    'k' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2: [symbol; 2] = ['i' as i32 as symbol, 'e' as i32 as symbol];
static mut s_3: [symbol; 2] = ['p' as i32 as symbol, 'o' as i32 as symbol];
static mut s_4: [symbol; 2] = ['p' as i32 as symbol, 'o' as i32 as symbol];
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    if out_grouping(
        z,
        &raw const g_V1 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        246 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    ) < 0 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = in_grouping(
        z,
        &raw const g_V1 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        246 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if ret < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c += ret;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
    if out_grouping(
        z,
        &raw const g_V1 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        246 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    ) < 0 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = in_grouping(
        z,
        &raw const g_V1 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        246 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c += ret_0;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_R2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(0 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_particle_etc(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_0 as *const among,
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
            if in_grouping_b(
                z,
                &raw const g_particle_end as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                246 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
        }
        2 => {
            let mut ret: ::core::ffi::c_int = r_R2(z);
            if ret <= 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        _ => {}
    }
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_possessive(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    among_var = find_among_b(z, &raw const a_4 as *const among, 9 as ::core::ffi::c_int);
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    match among_var {
        1 => {
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'k' as i32
            {
                (*z).c = (*z).l - m2;
            } else {
                (*z).c -= 1;
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
            (*z).ket = (*z).c;
            if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_0 as *const symbol) == 0 {
                return 0 as ::core::ffi::c_int;
            }
            (*z).bra = (*z).c;
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_1 as *const symbol,
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
        4 => {
            if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            if find_among_b(z, &raw const a_1 as *const among, 6 as ::core::ffi::c_int)
                == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_3: ::core::ffi::c_int = slice_del(z);
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        5 => {
            if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 228 as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            if find_among_b(z, &raw const a_2 as *const among, 6 as ::core::ffi::c_int)
                == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_4: ::core::ffi::c_int = slice_del(z);
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        6 => {
            if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 101 as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            if find_among_b(z, &raw const a_3 as *const among, 2 as ::core::ffi::c_int)
                == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_5: ::core::ffi::c_int = slice_del(z);
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_LONG(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if find_among_b(z, &raw const a_5 as *const among, 7 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_VI(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'i' as i32
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= 1;
    if in_grouping_b(
        z,
        &raw const g_V2 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        246 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_case_ending(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_6 as *const among,
        30 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    match among_var {
        1 => {
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'a' as i32
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
        }
        2 => {
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'e' as i32
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
        }
        3 => {
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'i' as i32
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
        }
        4 => {
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'o' as i32
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
        }
        5 => {
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 0xe4 as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
        }
        6 => {
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 0xf6 as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
        }
        7 => {
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret: ::core::ffi::c_int = r_LONG(z);
            if ret == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m4;
                if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_2 as *const symbol)
                    == 0
                {
                    (*z).c = (*z).l - m2;
                    current_block = 12497913735442871383;
                } else {
                    current_block = 11921368224509520691;
                }
            } else {
                if ret < 0 as ::core::ffi::c_int {
                    return ret;
                }
                current_block = 11921368224509520691;
            }
            match current_block {
                12497913735442871383 => {}
                _ => {
                    (*z).c = (*z).l - m3;
                    if (*z).c <= (*z).lb {
                        (*z).c = (*z).l - m2;
                    } else {
                        (*z).c -= 1;
                        (*z).bra = (*z).c;
                    }
                }
            }
        }
        8 => {
            if in_grouping_b(
                z,
                &raw const g_V1 as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                246 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            if in_grouping_b(
                z,
                &raw const g_C as *const ::core::ffi::c_uchar,
                98 as ::core::ffi::c_int,
                122 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
        }
        _ => {}
    }
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_other_endings(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_7 as *const among,
        14 as ::core::ffi::c_int,
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
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_3 as *const symbol) == 0 {
                (*z).c = (*z).l - m2;
            } else {
                return 0 as ::core::ffi::c_int
            }
        }
        _ => {}
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_i_plural(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 105 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 106 as ::core::ffi::c_int
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_8 as *const among, 2 as ::core::ffi::c_int) == 0 {
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
unsafe fn r_t_plural(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 't' as i32
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= 1;
    (*z).bra = (*z).c;
    let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
    if in_grouping_b(
        z,
        &raw const g_V1 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        246 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).c = (*z).l - m_test2;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = mlimit1;
    let mut mlimit3: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit3 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
    {
        (*z).lb = mlimit3;
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_9 as *const among, 2 as ::core::ffi::c_int);
    if among_var == 0 {
        (*z).lb = mlimit3;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit3;
    match among_var {
        1 => {
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_4 as *const symbol) == 0 {
                (*z).c = (*z).l - m4;
            } else {
                return 0 as ::core::ffi::c_int
            }
        }
        _ => {}
    }
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_tidy(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret: ::core::ffi::c_int = r_LONG(z);
    if !(ret == 0 as ::core::ffi::c_int) {
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
        (*z).c = (*z).l - m3;
        (*z).ket = (*z).c;
        if !((*z).c <= (*z).lb) {
            (*z).c -= 1;
            (*z).bra = (*z).c;
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
    }
    (*z).c = (*z).l - m2;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !(in_grouping_b(
        z,
        &raw const g_AEI as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        228 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0)
    {
        (*z).bra = (*z).c;
        if !(in_grouping_b(
            z,
            &raw const g_C as *const ::core::ffi::c_uchar,
            98 as ::core::ffi::c_int,
            122 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0)
        {
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
    }
    (*z).c = (*z).l - m4;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !((*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'j' as i32)
    {
        (*z).c -= 1;
        (*z).bra = (*z).c;
        let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 'o' as i32
        {
            (*z).c = (*z).l - m6;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'u' as i32
            {
                current_block = 8013683862412583581;
            } else {
                (*z).c -= 1;
                current_block = 3997580512369300716;
            }
        } else {
            (*z).c -= 1;
            current_block = 3997580512369300716;
        }
        match current_block {
            8013683862412583581 => {}
            _ => {
                let mut ret_2: ::core::ffi::c_int = slice_del(z);
                if ret_2 < 0 as ::core::ffi::c_int {
                    return ret_2;
                }
            }
        }
    }
    (*z).c = (*z).l - m5;
    let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !((*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'o' as i32)
    {
        (*z).c -= 1;
        (*z).bra = (*z).c;
        if !((*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 'j' as i32)
        {
            (*z).c -= 1;
            let mut ret_3: ::core::ffi::c_int = slice_del(z);
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
    }
    (*z).c = (*z).l - m7;
    (*z).lb = mlimit1;
    if in_grouping_b(
        z,
        &raw const g_V1 as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        246 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    ) < 0 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    if in_grouping_b(
        z,
        &raw const g_C as *const ::core::ffi::c_uchar,
        98 as ::core::ffi::c_int,
        122 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let ref mut fresh0 = *(*z).S.offset(0 as ::core::ffi::c_int as isize);
    *fresh0 = slice_to(z, *(*z).S.offset(0 as ::core::ffi::c_int as isize));
    if (*(*z).S.offset(0 as ::core::ffi::c_int as isize)).is_null() {
        return -(1 as ::core::ffi::c_int);
    }
    if eq_v_b(z, *(*z).S.offset(0 as ::core::ffi::c_int as isize)) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_4: ::core::ffi::c_int = slice_del(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn finnish_ISO_8859_1_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_mark_regions(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = c1;
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_particle_etc(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_possessive(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m3;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_case_ending(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m4;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_other_endings(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = (*z).l - m5;
    if *(*z).I.offset(2 as ::core::ffi::c_int as isize) == 0 {
        let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_5: ::core::ffi::c_int = r_t_plural(z);
        if ret_5 < 0 as ::core::ffi::c_int {
            return ret_5;
        }
        (*z).c = (*z).l - m7;
    } else {
        let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_4: ::core::ffi::c_int = r_i_plural(z);
        if ret_4 < 0 as ::core::ffi::c_int {
            return ret_4;
        }
        (*z).c = (*z).l - m6;
    }
    let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_6: ::core::ffi::c_int = r_tidy(z);
    if ret_6 < 0 as ::core::ffi::c_int {
        return ret_6;
    }
    (*z).c = (*z).l - m8;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn finnish_ISO_8859_1_create_env() -> *mut SN_env {
    return SN_create_env(1 as ::core::ffi::c_int, 3 as ::core::ffi::c_int);
}
pub unsafe fn finnish_ISO_8859_1_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 1 as ::core::ffi::c_int);
}
