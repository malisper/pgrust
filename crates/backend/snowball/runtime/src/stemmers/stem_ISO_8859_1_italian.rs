use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_1: [symbol; 2] = ['q' as i32 as symbol, 'u' as i32 as symbol];
static mut s_0_2: [symbol; 1] = [0xe1 as ::core::ffi::c_int as symbol];
static mut s_0_3: [symbol; 1] = [0xe9 as ::core::ffi::c_int as symbol];
static mut s_0_4: [symbol; 1] = [0xed as ::core::ffi::c_int as symbol];
static mut s_0_5: [symbol; 1] = [0xf3 as ::core::ffi::c_int as symbol];
static mut s_0_6: [symbol; 1] = [0xfa as ::core::ffi::c_int as symbol];
static mut a_0: [among; 7] = unsafe {
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
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_1: [symbol; 1] = ['I' as i32 as symbol];
static mut s_1_2: [symbol; 1] = ['U' as i32 as symbol];
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
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 2] = ['l' as i32 as symbol, 'a' as i32 as symbol];
static mut s_2_1: [symbol; 4] = [
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_2: [symbol; 6] = [
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_3: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_4: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_5: [symbol; 4] = [
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_6: [symbol; 2] = ['l' as i32 as symbol, 'e' as i32 as symbol];
static mut s_2_7: [symbol; 4] = [
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_8: [symbol; 6] = [
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_9: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_10: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_11: [symbol; 4] = [
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_12: [symbol; 2] = ['n' as i32 as symbol, 'e' as i32 as symbol];
static mut s_2_13: [symbol; 4] = [
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_14: [symbol; 6] = [
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_15: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_16: [symbol; 4] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_17: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_18: [symbol; 4] = [
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_19: [symbol; 2] = ['c' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_20: [symbol; 2] = ['l' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_21: [symbol; 4] = [
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_22: [symbol; 6] = [
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_23: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_24: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_25: [symbol; 4] = [
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_26: [symbol; 3] = [
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_27: [symbol; 2] = ['m' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_28: [symbol; 2] = ['s' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_29: [symbol; 2] = ['t' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_30: [symbol; 2] = ['v' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_31: [symbol; 2] = ['l' as i32 as symbol, 'o' as i32 as symbol];
static mut s_2_32: [symbol; 4] = [
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_33: [symbol; 6] = [
    'g' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_34: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_35: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_36: [symbol; 4] = [
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut a_2: [among; 37] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_11 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_14 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_15 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_16 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_17 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_18 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_21 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_22 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_23 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_24 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_25 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_26 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_32 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_33 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_34 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_35 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_36 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_1: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_3_2: [symbol; 2] = ['a' as i32 as symbol, 'r' as i32 as symbol];
static mut s_3_3: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_3_4: [symbol; 2] = ['i' as i32 as symbol, 'r' as i32 as symbol];
static mut a_3: [among; 5] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_4_1: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_4_2: [symbol; 2] = ['o' as i32 as symbol, 's' as i32 as symbol];
static mut s_4_3: [symbol; 2] = ['i' as i32 as symbol, 'v' as i32 as symbol];
static mut a_4: [among; 4] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
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
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_5_1: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_5_2: [symbol; 2] = ['i' as i32 as symbol, 'v' as i32 as symbol];
static mut a_5: [among; 3] = unsafe {
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_1: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_2: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_3: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_4: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_5: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_6: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_7: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_8: [symbol; 6] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_9: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_10: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_11: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_12: [symbol; 5] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_13: [symbol; 6] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_14: [symbol; 6] = [
    'a' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_15: [symbol; 6] = [
    'u' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_16: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_17: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_18: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_19: [symbol; 5] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_20: [symbol; 6] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_21: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_22: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_23: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_24: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_25: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_26: [symbol; 6] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_27: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'h' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_28: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_29: [symbol; 5] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_30: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_31: [symbol; 6] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_32: [symbol; 6] = [
    'a' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_33: [symbol; 6] = [
    'u' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_34: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_35: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_36: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_37: [symbol; 6] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_38: [symbol; 6] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_39: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_40: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_41: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_42: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_43: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_44: [symbol; 6] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_45: [symbol; 6] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_46: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_47: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
];
static mut s_6_48: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
];
static mut s_6_49: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    0xe8 as ::core::ffi::c_int as symbol,
];
static mut s_6_50: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    0xec as ::core::ffi::c_int as symbol,
];
static mut a_6: [among; 51] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_46 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_47 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_50 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_1: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_2: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_3: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_4: [symbol; 3] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_5: [symbol; 3] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_6: [symbol; 3] = [
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_7: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_8: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'b' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_9: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'b' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_10: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_11: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_12: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_13: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_14: [symbol; 3] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_15: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_16: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_17: [symbol; 5] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_18: [symbol; 5] = [
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_19: [symbol; 5] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_20: [symbol; 3] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_21: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_22: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_23: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_24: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_25: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_26: [symbol; 3] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_27: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_28: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_29: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_30: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_31: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_32: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_33: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_34: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_35: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_36: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_37: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_38: [symbol; 3] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_39: [symbol; 3] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_40: [symbol; 3] = [
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_41: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_7_42: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_43: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_44: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_45: [symbol; 4] = [
    'Y' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_46: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_47: [symbol; 5] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_48: [symbol; 5] = [
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_49: [symbol; 5] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_50: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_51: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_52: [symbol; 6] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_53: [symbol; 4] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_54: [symbol; 4] = [
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_55: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_56: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_57: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_58: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_59: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_60: [symbol; 5] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_61: [symbol; 5] = [
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_62: [symbol; 5] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_63: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_64: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_65: [symbol; 3] = [
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_66: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_67: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_68: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_69: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_70: [symbol; 8] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'b' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_71: [symbol; 8] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'b' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_72: [symbol; 6] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_73: [symbol; 6] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_74: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_75: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_76: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_77: [symbol; 3] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_78: [symbol; 3] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_79: [symbol; 3] = [
    'e' as i32 as symbol,
    'v' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_80: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_81: [symbol; 2] = ['a' as i32 as symbol, 'r' as i32 as symbol];
static mut s_7_82: [symbol; 2] = ['i' as i32 as symbol, 'r' as i32 as symbol];
static mut s_7_83: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
];
static mut s_7_84: [symbol; 3] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
];
static mut s_7_85: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xf2 as ::core::ffi::c_int as symbol,
];
static mut s_7_86: [symbol; 3] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xf2 as ::core::ffi::c_int as symbol,
];
static mut a_7: [among; 87] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_18 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_19 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_21 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_22 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_46 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_47 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_50 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_51 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_53 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_55 as *const symbol,
            substring_i: 54 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_56 as *const symbol,
            substring_i: 54 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_57 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_58 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_59 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_60 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_61 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_62 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_63 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_64 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_65 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_66 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_67 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_68 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_69 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_7_70 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_7_71 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_72 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_73 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_74 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_75 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_76 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_77 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_78 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_79 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_80 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_81 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_82 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_83 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_84 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_85 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_86 as *const symbol,
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
    128 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    2 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_AEIO: [::core::ffi::c_uchar; 19] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
    128 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    2 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_CG: [::core::ffi::c_uchar; 1] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 1] = [0xe0 as ::core::ffi::c_int as symbol];
static mut s_1: [symbol; 1] = [0xe8 as ::core::ffi::c_int as symbol];
static mut s_2: [symbol; 1] = [0xec as ::core::ffi::c_int as symbol];
static mut s_3: [symbol; 1] = [0xf2 as ::core::ffi::c_int as symbol];
static mut s_4: [symbol; 1] = [0xf9 as ::core::ffi::c_int as symbol];
static mut s_5: [symbol; 2] = ['q' as i32 as symbol, 'U' as i32 as symbol];
static mut s_6: [symbol; 1] = ['U' as i32 as symbol];
static mut s_7: [symbol; 1] = ['I' as i32 as symbol];
static mut s_8: [symbol; 1] = ['i' as i32 as symbol];
static mut s_9: [symbol; 1] = ['u' as i32 as symbol];
static mut s_10: [symbol; 1] = ['e' as i32 as symbol];
static mut s_11: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_12: [symbol; 3] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_13: [symbol; 1] = ['u' as i32 as symbol];
static mut s_14: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_15: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_16: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_17: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_18: [symbol; 6] = [
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_19: [symbol; 5] = [
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
unsafe fn r_prelude(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    let mut c_test1: ::core::ffi::c_int = (*z).c;
    loop {
        let mut c2: ::core::ffi::c_int = (*z).c;
        (*z).bra = (*z).c;
        among_var = find_among(
            z,
            &raw const a_0 as *const among,
            7 as ::core::ffi::c_int,
        );
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
                    2 as ::core::ffi::c_int,
                    &raw const s_5 as *const symbol,
                );
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
            }
            7 => {
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
    's_161: loop {
        let mut c3: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c4: ::core::ffi::c_int = (*z).c;
            if !(in_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                249 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0)
            {
                (*z).bra = (*z).c;
                let mut c5: ::core::ffi::c_int = (*z).c;
                if (*z).c == (*z).l
                    || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                        != 'u' as i32
                {
                    current_block = 6541682870829540387;
                } else {
                    (*z).c += 1;
                    (*z).ket = (*z).c;
                    if in_grouping(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        249 as ::core::ffi::c_int,
                        0 as ::core::ffi::c_int,
                    ) != 0
                    {
                        current_block = 6541682870829540387;
                    } else {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            1 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 241256982970103214;
                    }
                }
                match current_block {
                    6541682870829540387 => {
                        (*z).c = c5;
                        if (*z).c == (*z).l
                            || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                                != 'i' as i32
                        {
                            current_block = 1668397238821395122;
                        } else {
                            (*z).c += 1;
                            (*z).ket = (*z).c;
                            if in_grouping(
                                z,
                                &raw const g_v as *const ::core::ffi::c_uchar,
                                97 as ::core::ffi::c_int,
                                249 as ::core::ffi::c_int,
                                0 as ::core::ffi::c_int,
                            ) != 0
                            {
                                current_block = 1668397238821395122;
                            } else {
                                let mut ret_6: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    1 as ::core::ffi::c_int,
                                    &raw const s_7 as *const symbol,
                                );
                                if ret_6 < 0 as ::core::ffi::c_int {
                                    return ret_6;
                                }
                                current_block = 241256982970103214;
                            }
                        }
                    }
                    _ => {}
                }
                match current_block {
                    1668397238821395122 => {}
                    _ => {
                        (*z).c = c4;
                        continue 's_161;
                    }
                }
            }
            (*z).c = c4;
            if (*z).c >= (*z).l {
                break;
            }
            (*z).c += 1;
        }
        (*z).c = c3;
        break;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut c2: ::core::ffi::c_int = (*z).c;
    if in_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        249 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 10589157573061528300;
    } else {
        let mut c3: ::core::ffi::c_int = (*z).c;
        if out_grouping(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            249 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0
        {
            current_block = 5546918194407844401;
        } else {
            let mut ret: ::core::ffi::c_int = out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                249 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                current_block = 5546918194407844401;
            } else {
                (*z).c += ret;
                current_block = 13833906959802962017;
            }
        }
        match current_block {
            13833906959802962017 => {}
            _ => {
                (*z).c = c3;
                if in_grouping(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    249 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0
                {
                    current_block = 10589157573061528300;
                } else {
                    let mut ret_0: ::core::ffi::c_int = in_grouping(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        249 as ::core::ffi::c_int,
                        1 as ::core::ffi::c_int,
                    );
                    if ret_0 < 0 as ::core::ffi::c_int {
                        current_block = 10589157573061528300;
                    } else {
                        (*z).c += ret_0;
                        current_block = 13833906959802962017;
                    }
                }
            }
        }
    }
    match current_block {
        10589157573061528300 => {
            (*z).c = c2;
            if out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                249 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                current_block = 8078423370322375837;
            } else {
                let mut c4: ::core::ffi::c_int = (*z).c;
                if out_grouping(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    249 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0
                {
                    current_block = 11612076850871436666;
                } else {
                    let mut ret_1: ::core::ffi::c_int = out_grouping(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        249 as ::core::ffi::c_int,
                        1 as ::core::ffi::c_int,
                    );
                    if ret_1 < 0 as ::core::ffi::c_int {
                        current_block = 11612076850871436666;
                    } else {
                        (*z).c += ret_1;
                        current_block = 13833906959802962017;
                    }
                }
                match current_block {
                    13833906959802962017 => {}
                    _ => {
                        (*z).c = c4;
                        if in_grouping(
                            z,
                            &raw const g_v as *const ::core::ffi::c_uchar,
                            97 as ::core::ffi::c_int,
                            249 as ::core::ffi::c_int,
                            0 as ::core::ffi::c_int,
                        ) != 0
                        {
                            current_block = 8078423370322375837;
                        } else if (*z).c >= (*z).l {
                            current_block = 8078423370322375837;
                        } else {
                            (*z).c += 1;
                            current_block = 13833906959802962017;
                        }
                    }
                }
            }
        }
        _ => {}
    }
    match current_block {
        13833906959802962017 => {
            *(*z).I.offset(2 as ::core::ffi::c_int as isize) = (*z).c;
        }
        _ => {}
    }
    (*z).c = c1;
    let mut c5: ::core::ffi::c_int = (*z).c;
    let mut ret_2: ::core::ffi::c_int = out_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        249 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret_2 < 0 as ::core::ffi::c_int) {
        (*z).c += ret_2;
        let mut ret_3: ::core::ffi::c_int = in_grouping(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            249 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_3 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_3;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
            let mut ret_4: ::core::ffi::c_int = out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                249 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_4 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_4;
                let mut ret_5: ::core::ffi::c_int = in_grouping(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    249 as ::core::ffi::c_int,
                    1 as ::core::ffi::c_int,
                );
                if !(ret_5 < 0 as ::core::ffi::c_int) {
                    (*z).c += ret_5;
                    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
                }
            }
        }
    }
    (*z).c = c5;
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
                    as ::core::ffi::c_int != 85 as ::core::ffi::c_int
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
unsafe fn r_attached_pronoun(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 33314 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_2 as *const among, 37 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 111 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 114 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_3 as *const among, 5 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = r_RV(z);
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
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_standard_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_6 as *const among,
        51 as ::core::ffi::c_int,
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
            let mut ret_1: ::core::ffi::c_int = r_R2(z);
            if ret_1 <= 0 as ::core::ffi::c_int {
                return ret_1;
            }
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_11 as *const symbol) == 0
            {
                (*z).c = (*z).l - m1;
            } else {
                (*z).bra = (*z).c;
                let mut ret_3: ::core::ffi::c_int = r_R2(z);
                if ret_3 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m1;
                } else {
                    if ret_3 < 0 as ::core::ffi::c_int {
                        return ret_3;
                    }
                    let mut ret_4: ::core::ffi::c_int = slice_del(z);
                    if ret_4 < 0 as ::core::ffi::c_int {
                        return ret_4;
                    }
                }
            }
        }
        3 => {
            let mut ret_5: ::core::ffi::c_int = r_R2(z);
            if ret_5 <= 0 as ::core::ffi::c_int {
                return ret_5;
            }
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_12 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        4 => {
            let mut ret_7: ::core::ffi::c_int = r_R2(z);
            if ret_7 <= 0 as ::core::ffi::c_int {
                return ret_7;
            }
            let mut ret_8: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_13 as *const symbol,
            );
            if ret_8 < 0 as ::core::ffi::c_int {
                return ret_8;
            }
        }
        5 => {
            let mut ret_9: ::core::ffi::c_int = r_R2(z);
            if ret_9 <= 0 as ::core::ffi::c_int {
                return ret_9;
            }
            let mut ret_10: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_14 as *const symbol,
            );
            if ret_10 < 0 as ::core::ffi::c_int {
                return ret_10;
            }
        }
        6 => {
            let mut ret_11: ::core::ffi::c_int = r_RV(z);
            if ret_11 <= 0 as ::core::ffi::c_int {
                return ret_11;
            }
            let mut ret_12: ::core::ffi::c_int = slice_del(z);
            if ret_12 < 0 as ::core::ffi::c_int {
                return ret_12;
            }
        }
        7 => {
            let mut ret_13: ::core::ffi::c_int = r_R1(z);
            if ret_13 <= 0 as ::core::ffi::c_int {
                return ret_13;
            }
            let mut ret_14: ::core::ffi::c_int = slice_del(z);
            if ret_14 < 0 as ::core::ffi::c_int {
                return ret_14;
            }
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                    != 3 as ::core::ffi::c_int
                || 4722696 as ::core::ffi::c_int
                    >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                    & 1 as ::core::ffi::c_int == 0
            {
                (*z).c = (*z).l - m2;
            } else {
                among_var = find_among_b(
                    z,
                    &raw const a_4 as *const among,
                    4 as ::core::ffi::c_int,
                );
                if among_var == 0 {
                    (*z).c = (*z).l - m2;
                } else {
                    (*z).bra = (*z).c;
                    let mut ret_15: ::core::ffi::c_int = r_R2(z);
                    if ret_15 == 0 as ::core::ffi::c_int {
                        (*z).c = (*z).l - m2;
                    } else {
                        if ret_15 < 0 as ::core::ffi::c_int {
                            return ret_15;
                        }
                        let mut ret_16: ::core::ffi::c_int = slice_del(z);
                        if ret_16 < 0 as ::core::ffi::c_int {
                            return ret_16;
                        }
                        match among_var {
                            1 => {
                                (*z).ket = (*z).c;
                                if eq_s_b(
                                    z,
                                    2 as ::core::ffi::c_int,
                                    &raw const s_15 as *const symbol,
                                ) == 0
                                {
                                    (*z).c = (*z).l - m2;
                                } else {
                                    (*z).bra = (*z).c;
                                    let mut ret_17: ::core::ffi::c_int = r_R2(z);
                                    if ret_17 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m2;
                                    } else {
                                        if ret_17 < 0 as ::core::ffi::c_int {
                                            return ret_17;
                                        }
                                        let mut ret_18: ::core::ffi::c_int = slice_del(z);
                                        if ret_18 < 0 as ::core::ffi::c_int {
                                            return ret_18;
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        8 => {
            let mut ret_19: ::core::ffi::c_int = r_R2(z);
            if ret_19 <= 0 as ::core::ffi::c_int {
                return ret_19;
            }
            let mut ret_20: ::core::ffi::c_int = slice_del(z);
            if ret_20 < 0 as ::core::ffi::c_int {
                return ret_20;
            }
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                    != 3 as ::core::ffi::c_int
                || 4198408 as ::core::ffi::c_int
                    >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                    & 1 as ::core::ffi::c_int == 0
            {
                (*z).c = (*z).l - m3;
            } else if find_among_b(
                z,
                &raw const a_5 as *const among,
                3 as ::core::ffi::c_int,
            ) == 0
            {
                (*z).c = (*z).l - m3;
            } else {
                (*z).bra = (*z).c;
                let mut ret_21: ::core::ffi::c_int = r_R2(z);
                if ret_21 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m3;
                } else {
                    if ret_21 < 0 as ::core::ffi::c_int {
                        return ret_21;
                    }
                    let mut ret_22: ::core::ffi::c_int = slice_del(z);
                    if ret_22 < 0 as ::core::ffi::c_int {
                        return ret_22;
                    }
                }
            }
        }
        9 => {
            let mut ret_23: ::core::ffi::c_int = r_R2(z);
            if ret_23 <= 0 as ::core::ffi::c_int {
                return ret_23;
            }
            let mut ret_24: ::core::ffi::c_int = slice_del(z);
            if ret_24 < 0 as ::core::ffi::c_int {
                return ret_24;
            }
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_16 as *const symbol) == 0
            {
                (*z).c = (*z).l - m4;
            } else {
                (*z).bra = (*z).c;
                let mut ret_25: ::core::ffi::c_int = r_R2(z);
                if ret_25 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m4;
                } else {
                    if ret_25 < 0 as ::core::ffi::c_int {
                        return ret_25;
                    }
                    let mut ret_26: ::core::ffi::c_int = slice_del(z);
                    if ret_26 < 0 as ::core::ffi::c_int {
                        return ret_26;
                    }
                    (*z).ket = (*z).c;
                    if eq_s_b(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_17 as *const symbol,
                    ) == 0
                    {
                        (*z).c = (*z).l - m4;
                    } else {
                        (*z).bra = (*z).c;
                        let mut ret_27: ::core::ffi::c_int = r_R2(z);
                        if ret_27 == 0 as ::core::ffi::c_int {
                            (*z).c = (*z).l - m4;
                        } else {
                            if ret_27 < 0 as ::core::ffi::c_int {
                                return ret_27;
                            }
                            let mut ret_28: ::core::ffi::c_int = slice_del(z);
                            if ret_28 < 0 as ::core::ffi::c_int {
                                return ret_28;
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_verb_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(2 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(2 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_7 as *const among, 87 as ::core::ffi::c_int) == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = mlimit1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_vowel_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if in_grouping_b(
        z,
        &raw const g_AEIO as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        242 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        (*z).c = (*z).l - m1;
    } else {
        (*z).bra = (*z).c;
        let mut ret: ::core::ffi::c_int = r_RV(z);
        if ret == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m1;
        } else {
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'i' as i32
            {
                (*z).c = (*z).l - m1;
            } else {
                (*z).c -= 1;
                (*z).bra = (*z).c;
                let mut ret_1: ::core::ffi::c_int = r_RV(z);
                if ret_1 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m1;
                } else {
                    if ret_1 < 0 as ::core::ffi::c_int {
                        return ret_1;
                    }
                    let mut ret_2: ::core::ffi::c_int = slice_del(z);
                    if ret_2 < 0 as ::core::ffi::c_int {
                        return ret_2;
                    }
                }
            }
        }
    }
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'h' as i32
    {
        (*z).c = (*z).l - m2;
    } else {
        (*z).c -= 1;
        (*z).bra = (*z).c;
        if in_grouping_b(
            z,
            &raw const g_CG as *const ::core::ffi::c_uchar,
            99 as ::core::ffi::c_int,
            103 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0
        {
            (*z).c = (*z).l - m2;
        } else {
            let mut ret_3: ::core::ffi::c_int = r_RV(z);
            if ret_3 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m2;
            } else {
                if ret_3 < 0 as ::core::ffi::c_int {
                    return ret_3;
                }
                let mut ret_4: ::core::ffi::c_int = slice_del(z);
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
            }
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_exceptions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).bra = (*z).c;
    if eq_s(z, 6 as ::core::ffi::c_int, &raw const s_18 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c < (*z).l {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_from_s(
        z,
        5 as ::core::ffi::c_int,
        &raw const s_19 as *const symbol,
    );
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn italian_ISO_8859_1_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_exceptions(z);
    if ret == 0 as ::core::ffi::c_int {
        (*z).c = c1;
        let mut c2: ::core::ffi::c_int = (*z).c;
        let mut ret_0: ::core::ffi::c_int = r_prelude(z);
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
        (*z).c = c2;
        let mut ret_1: ::core::ffi::c_int = r_mark_regions(z);
        if ret_1 < 0 as ::core::ffi::c_int {
            return ret_1;
        }
        (*z).lb = (*z).c;
        (*z).c = (*z).l;
        let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_2: ::core::ffi::c_int = r_attached_pronoun(z);
        if ret_2 < 0 as ::core::ffi::c_int {
            return ret_2;
        }
        (*z).c = (*z).l - m3;
        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_3: ::core::ffi::c_int = r_standard_suffix(z);
        if ret_3 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m5;
            let mut ret_4: ::core::ffi::c_int = r_verb_suffix(z);
            if !(ret_4 == 0 as ::core::ffi::c_int) {
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
            }
        } else if ret_3 < 0 as ::core::ffi::c_int {
            return ret_3
        }
        (*z).c = (*z).l - m4;
        let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_5: ::core::ffi::c_int = r_vowel_suffix(z);
        if ret_5 < 0 as ::core::ffi::c_int {
            return ret_5;
        }
        (*z).c = (*z).l - m6;
        (*z).c = (*z).lb;
        let mut c7: ::core::ffi::c_int = (*z).c;
        let mut ret_6: ::core::ffi::c_int = r_postlude(z);
        if ret_6 < 0 as ::core::ffi::c_int {
            return ret_6;
        }
        (*z).c = c7;
    } else if ret < 0 as ::core::ffi::c_int {
        return ret
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn italian_ISO_8859_1_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 3 as ::core::ffi::c_int);
}
pub unsafe fn italian_ISO_8859_1_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
