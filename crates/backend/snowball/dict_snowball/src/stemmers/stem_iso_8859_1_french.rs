use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 3] = [
    'c' as i32 as symbol,
    'o' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_0_1: [symbol; 3] = [
    'p' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_0_2: [symbol; 3] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'p' as i32 as symbol,
];
static mut a_0: [among; 3] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_1_1: [symbol; 1] = ['H' as i32 as symbol];
static mut s_1_2: [symbol; 2] = ['H' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_3: [symbol; 2] = ['H' as i32 as symbol, 'i' as i32 as symbol];
static mut s_1_4: [symbol; 1] = ['I' as i32 as symbol];
static mut s_1_5: [symbol; 1] = ['U' as i32 as symbol];
static mut s_1_6: [symbol; 1] = ['Y' as i32 as symbol];
static mut a_1: [among; 7] = unsafe {
    [
        among {
            s_size: 0 as ::core::ffi::c_int,
            s: ::core::ptr::null::<symbol>(),
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 3] = [
    'i' as i32 as symbol,
    'q' as i32 as symbol,
    'U' as i32 as symbol,
];
static mut s_2_1: [symbol; 3] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_2_2: [symbol; 3] = [
    'I' as i32 as symbol,
    0xe8 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
];
static mut s_2_3: [symbol; 3] = [
    'i' as i32 as symbol,
    0xe8 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
];
static mut s_2_4: [symbol; 3] = [
    'e' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_5: [symbol; 2] = ['i' as i32 as symbol, 'v' as i32 as symbol];
static mut a_2: [among; 6] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_3_1: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_3_2: [symbol; 2] = ['i' as i32 as symbol, 'v' as i32 as symbol];
static mut a_3: [among; 3] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
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
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 4] = [
    'i' as i32 as symbol,
    'q' as i32 as symbol,
    'U' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_1: [symbol; 6] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_2: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_3: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_4: [symbol; 5] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_5: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_6: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_7: [symbol; 4] = [
    'e' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_8: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_9: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_10: [symbol; 2] = ['i' as i32 as symbol, 'f' as i32 as symbol];
static mut s_4_11: [symbol; 5] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_4_12: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_4_13: [symbol; 5] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_4_14: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_4_15: [symbol; 5] = [
    'i' as i32 as symbol,
    'q' as i32 as symbol,
    'U' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_16: [symbol; 7] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_17: [symbol; 5] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_18: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_19: [symbol; 6] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_20: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_21: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_22: [symbol; 5] = [
    'e' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_23: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_24: [symbol; 4] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_25: [symbol; 3] = [
    'i' as i32 as symbol,
    'f' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_26: [symbol; 6] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_27: [symbol; 6] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_28: [symbol; 6] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_29: [symbol; 6] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_30: [symbol; 5] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_31: [symbol; 6] = [
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_32: [symbol; 9] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4_33: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_4_34: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_35: [symbol; 5] = [
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_36: [symbol; 8] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_37: [symbol; 6] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_38: [symbol; 6] = [
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_39: [symbol; 3] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    'x' as i32 as symbol,
];
static mut s_4_40: [symbol; 4] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    'x' as i32 as symbol,
];
static mut s_4_41: [symbol; 3] = [
    'e' as i32 as symbol,
    'u' as i32 as symbol,
    'x' as i32 as symbol,
];
static mut s_4_42: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xe9 as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 43] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_4_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_31 as *const symbol,
            substring_i: 30 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_4_32 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_35 as *const symbol,
            substring_i: 34 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_4_36 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_37 as *const symbol,
            substring_i: 34 as ::core::ffi::c_int,
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_38 as *const symbol,
            substring_i: 34 as ::core::ffi::c_int,
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_40 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 3] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_5_1: [symbol; 2] = ['i' as i32 as symbol, 'e' as i32 as symbol];
static mut s_5_2: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_3: [symbol; 7] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_4: [symbol; 1] = ['i' as i32 as symbol];
static mut s_5_5: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_6: [symbol; 2] = ['i' as i32 as symbol, 'r' as i32 as symbol];
static mut s_5_7: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_8: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_9: [symbol; 4] = [
    0xee as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_10: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_11: [symbol; 8] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_12: [symbol; 4] = [
    0xee as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_13: [symbol; 2] = ['i' as i32 as symbol, 's' as i32 as symbol];
static mut s_5_14: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_15: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_16: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_17: [symbol; 7] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_18: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_19: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_20: [symbol; 7] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5_21: [symbol; 2] = ['i' as i32 as symbol, 't' as i32 as symbol];
static mut s_5_22: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_23: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_24: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_25: [symbol; 7] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'I' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_26: [symbol; 8] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'I' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_27: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_28: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_29: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_5_30: [symbol; 2] = [
    0xee as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
];
static mut s_5_31: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_5_32: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_5_33: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_5_34: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut a_5: [among; 35] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_5_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_14 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_15 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_22 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_23 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_5_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_6_1: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_2: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_3: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_4: [symbol; 2] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
];
static mut s_6_5: [symbol; 2] = ['a' as i32 as symbol, 'i' as i32 as symbol];
static mut s_6_6: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_6_7: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_6_8: [symbol; 2] = ['a' as i32 as symbol, 's' as i32 as symbol];
static mut s_6_9: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_10: [symbol; 4] = [
    0xe2 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_11: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_12: [symbol; 5] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_13: [symbol; 4] = [
    0xe2 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_14: [symbol; 3] = [
    0xe9 as ::core::ffi::c_int as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_15: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_16: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_17: [symbol; 4] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_18: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_19: [symbol; 7] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_20: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_21: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_22: [symbol; 2] = [
    0xe9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_6_23: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_6_24: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_6_25: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_6_26: [symbol; 5] = [
    'a' as i32 as symbol,
    'I' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_6_27: [symbol; 7] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'I' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_6_28: [symbol; 5] = [
    0xe8 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_6_29: [symbol; 6] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_6_30: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_6_31: [symbol; 2] = [
    0xe2 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
];
static mut s_6_32: [symbol; 2] = ['e' as i32 as symbol, 'z' as i32 as symbol];
static mut s_6_33: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_6_34: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_6_35: [symbol; 6] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_6_36: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_6_37: [symbol; 1] = [0xe9 as ::core::ffi::c_int as symbol];
static mut a_6: [among; 38] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_6_19 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_24 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_6_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_33 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_34 as *const symbol,
            substring_i: 33 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_35 as *const symbol,
            substring_i: 33 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_36 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_6_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 1] = ['e' as i32 as symbol];
static mut s_7_1: [symbol; 4] = [
    'I' as i32 as symbol,
    0xe8 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_2: [symbol; 4] = [
    'i' as i32 as symbol,
    0xe8 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_7_3: [symbol; 3] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_7_4: [symbol; 3] = [
    'I' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_7_5: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut a_7: [among; 6] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
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
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_8_0: [symbol; 3] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_8_1: [symbol; 4] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_8_2: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_3: [symbol; 3] = [
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_4: [symbol; 3] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    't' as i32 as symbol,
];
static mut a_8: [among; 5] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 20] = [
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
    128 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    130 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    103 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    5 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_elision_char: [::core::ffi::c_uchar; 3] = [
    131 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    14 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    3 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_keep_with_s: [::core::ffi::c_uchar; 17] = [
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    20 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
];
static mut s_0: [symbol; 2] = ['q' as i32 as symbol, 'u' as i32 as symbol];
static mut s_1: [symbol; 1] = ['U' as i32 as symbol];
static mut s_2: [symbol; 1] = ['I' as i32 as symbol];
static mut s_3: [symbol; 1] = ['Y' as i32 as symbol];
static mut s_4: [symbol; 2] = ['H' as i32 as symbol, 'e' as i32 as symbol];
static mut s_5: [symbol; 2] = ['H' as i32 as symbol, 'i' as i32 as symbol];
static mut s_6: [symbol; 1] = ['Y' as i32 as symbol];
static mut s_7: [symbol; 1] = ['U' as i32 as symbol];
static mut s_8: [symbol; 1] = ['i' as i32 as symbol];
static mut s_9: [symbol; 1] = ['u' as i32 as symbol];
static mut s_10: [symbol; 1] = ['y' as i32 as symbol];
static mut s_11: [symbol; 1] = [0xeb as ::core::ffi::c_int as symbol];
static mut s_12: [symbol; 1] = [0xef as ::core::ffi::c_int as symbol];
static mut s_13: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_14: [symbol; 3] = [
    'i' as i32 as symbol,
    'q' as i32 as symbol,
    'U' as i32 as symbol,
];
static mut s_15: [symbol; 3] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_16: [symbol; 1] = ['u' as i32 as symbol];
static mut s_17: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_18: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_19: [symbol; 3] = [
    'e' as i32 as symbol,
    'u' as i32 as symbol,
    'x' as i32 as symbol,
];
static mut s_20: [symbol; 1] = ['i' as i32 as symbol];
static mut s_21: [symbol; 3] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_22: [symbol; 3] = [
    'i' as i32 as symbol,
    'q' as i32 as symbol,
    'U' as i32 as symbol,
];
static mut s_23: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_24: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_25: [symbol; 3] = [
    'i' as i32 as symbol,
    'q' as i32 as symbol,
    'U' as i32 as symbol,
];
static mut s_26: [symbol; 3] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_27: [symbol; 2] = ['a' as i32 as symbol, 'l' as i32 as symbol];
static mut s_28: [symbol; 3] = [
    'e' as i32 as symbol,
    'u' as i32 as symbol,
    'x' as i32 as symbol,
];
static mut s_29: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_30: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_31: [symbol; 2] = ['H' as i32 as symbol, 'i' as i32 as symbol];
static mut s_32: [symbol; 1] = ['i' as i32 as symbol];
static mut s_33: [symbol; 1] = ['e' as i32 as symbol];
static mut s_34: [symbol; 1] = ['i' as i32 as symbol];
static mut s_35: [symbol; 1] = ['c' as i32 as symbol];
unsafe fn r_elisions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).bra = (*z).c;
    let mut c1: ::core::ffi::c_int = (*z).c;
    if in_grouping(
        z,
        &raw const g_elision_char as *const ::core::ffi::c_uchar,
        99 as ::core::ffi::c_int,
        116 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        (*z).c = c1;
        if eq_s(z, 2 as ::core::ffi::c_int, &raw const s_0 as *const symbol) == 0 {
            return 0 as ::core::ffi::c_int;
        }
    }
    if (*z).c == (*z).l
        || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int != '\'' as i32
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c += 1;
    (*z).ket = (*z).c;
    if (*z).c < (*z).l {
        let mut ret: ::core::ffi::c_int = slice_del(z);
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
        return 1 as ::core::ffi::c_int;
    } else {
        return 0 as ::core::ffi::c_int
    };
}
unsafe fn r_prelude(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    's_4: loop {
        let mut c1: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c2: ::core::ffi::c_int = (*z).c;
            let mut c3: ::core::ffi::c_int = (*z).c;
            if in_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                251 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                current_block = 17147356961908396225;
            } else {
                (*z).bra = (*z).c;
                let mut c4: ::core::ffi::c_int = (*z).c;
                if (*z).c == (*z).l
                    || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                        != 'u' as i32
                {
                    current_block = 14356106777580902465;
                } else {
                    (*z).c += 1;
                    (*z).ket = (*z).c;
                    if in_grouping(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        251 as ::core::ffi::c_int,
                        0 as ::core::ffi::c_int,
                    ) != 0
                    {
                        current_block = 14356106777580902465;
                    } else {
                        let mut ret: ::core::ffi::c_int = slice_from_s(
                            z,
                            1 as ::core::ffi::c_int,
                            &raw const s_1 as *const symbol,
                        );
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 11353488674323798774;
                    }
                }
                match current_block {
                    11353488674323798774 => {}
                    _ => {
                        (*z).c = c4;
                        if (*z).c == (*z).l
                            || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                                != 'i' as i32
                        {
                            current_block = 1651874675934181524;
                        } else {
                            (*z).c += 1;
                            (*z).ket = (*z).c;
                            if in_grouping(
                                z,
                                &raw const g_v as *const ::core::ffi::c_uchar,
                                97 as ::core::ffi::c_int,
                                251 as ::core::ffi::c_int,
                                0 as ::core::ffi::c_int,
                            ) != 0
                            {
                                current_block = 1651874675934181524;
                            } else {
                                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    1 as ::core::ffi::c_int,
                                    &raw const s_2 as *const symbol,
                                );
                                if ret_0 < 0 as ::core::ffi::c_int {
                                    return ret_0;
                                }
                                current_block = 11353488674323798774;
                            }
                        }
                        match current_block {
                            11353488674323798774 => {}
                            _ => {
                                (*z).c = c4;
                                if (*z).c == (*z).l
                                    || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                                        != 'y' as i32
                                {
                                    current_block = 17147356961908396225;
                                } else {
                                    (*z).c += 1;
                                    (*z).ket = (*z).c;
                                    let mut ret_1: ::core::ffi::c_int = slice_from_s(
                                        z,
                                        1 as ::core::ffi::c_int,
                                        &raw const s_3 as *const symbol,
                                    );
                                    if ret_1 < 0 as ::core::ffi::c_int {
                                        return ret_1;
                                    }
                                    current_block = 11353488674323798774;
                                }
                            }
                        }
                    }
                }
            }
            match current_block {
                17147356961908396225 => {
                    (*z).c = c3;
                    (*z).bra = (*z).c;
                    if (*z).c == (*z).l
                        || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                            != 0xeb as ::core::ffi::c_int
                    {
                        (*z).c = c3;
                        (*z).bra = (*z).c;
                        if (*z).c == (*z).l
                            || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                                != 0xef as ::core::ffi::c_int
                        {
                            (*z).c = c3;
                            (*z).bra = (*z).c;
                            if (*z).c == (*z).l
                                || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                                    != 'y' as i32
                            {
                                current_block = 12779558415053401456;
                            } else {
                                (*z).c += 1;
                                (*z).ket = (*z).c;
                                if in_grouping(
                                    z,
                                    &raw const g_v as *const ::core::ffi::c_uchar,
                                    97 as ::core::ffi::c_int,
                                    251 as ::core::ffi::c_int,
                                    0 as ::core::ffi::c_int,
                                ) != 0
                                {
                                    current_block = 12779558415053401456;
                                } else {
                                    let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                        z,
                                        1 as ::core::ffi::c_int,
                                        &raw const s_6 as *const symbol,
                                    );
                                    if ret_4 < 0 as ::core::ffi::c_int {
                                        return ret_4;
                                    }
                                    current_block = 11353488674323798774;
                                }
                            }
                            match current_block {
                                11353488674323798774 => {}
                                _ => {
                                    (*z).c = c3;
                                    if (*z).c == (*z).l
                                        || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                                            != 'q' as i32
                                    {
                                        current_block = 17840797975588709955;
                                    } else {
                                        (*z).c += 1;
                                        (*z).bra = (*z).c;
                                        if (*z).c == (*z).l
                                            || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                                                != 'u' as i32
                                        {
                                            current_block = 17840797975588709955;
                                        } else {
                                            (*z).c += 1;
                                            (*z).ket = (*z).c;
                                            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                                                z,
                                                1 as ::core::ffi::c_int,
                                                &raw const s_7 as *const symbol,
                                            );
                                            if ret_5 < 0 as ::core::ffi::c_int {
                                                return ret_5;
                                            }
                                            current_block = 11353488674323798774;
                                        }
                                    }
                                    match current_block {
                                        11353488674323798774 => {}
                                        _ => {
                                            (*z).c = c2;
                                            if (*z).c >= (*z).l {
                                                break;
                                            }
                                            (*z).c += 1;
                                            continue;
                                        }
                                    }
                                }
                            }
                        } else {
                            (*z).c += 1;
                            (*z).ket = (*z).c;
                            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                                z,
                                2 as ::core::ffi::c_int,
                                &raw const s_5 as *const symbol,
                            );
                            if ret_3 < 0 as ::core::ffi::c_int {
                                return ret_3;
                            }
                        }
                    } else {
                        (*z).c += 1;
                        (*z).ket = (*z).c;
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            2 as ::core::ffi::c_int,
                            &raw const s_4 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                    }
                }
                _ => {}
            }
            (*z).c = c2;
            continue 's_4;
        }
        (*z).c = c1;
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
        251 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 17084860842997001940;
    } else if in_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        251 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 17084860842997001940;
    } else if (*z).c >= (*z).l {
        current_block = 17084860842997001940;
    } else {
        (*z).c += 1;
        current_block = 17810831551857678258;
    }
    match current_block {
        17084860842997001940 => {
            (*z).c = c2;
            if (*z).c + 2 as ::core::ffi::c_int >= (*z).l
                || *(*z).p.offset(((*z).c + 2 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                    != 3 as ::core::ffi::c_int
                || 331776 as ::core::ffi::c_int
                    >> (*(*z).p.offset(((*z).c + 2 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                    & 1 as ::core::ffi::c_int == 0
            {
                current_block = 16185823415883628448;
            } else if find_among(
                z,
                &raw const a_0 as *const among,
                3 as ::core::ffi::c_int,
            ) == 0
            {
                current_block = 16185823415883628448;
            } else {
                current_block = 17810831551857678258;
            }
            match current_block {
                17810831551857678258 => {}
                _ => {
                    (*z).c = c2;
                    if (*z).c >= (*z).l {
                        current_block = 4839879196257035163;
                    } else {
                        (*z).c += 1;
                        let mut ret: ::core::ffi::c_int = out_grouping(
                            z,
                            &raw const g_v as *const ::core::ffi::c_uchar,
                            97 as ::core::ffi::c_int,
                            251 as ::core::ffi::c_int,
                            1 as ::core::ffi::c_int,
                        );
                        if ret < 0 as ::core::ffi::c_int {
                            current_block = 4839879196257035163;
                        } else {
                            (*z).c += ret;
                            current_block = 17810831551857678258;
                        }
                    }
                }
            }
        }
        _ => {}
    }
    match current_block {
        17810831551857678258 => {
            *(*z).I.offset(2 as ::core::ffi::c_int as isize) = (*z).c;
        }
        _ => {}
    }
    (*z).c = c1;
    let mut c3: ::core::ffi::c_int = (*z).c;
    let mut ret_0: ::core::ffi::c_int = out_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        251 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret_0 < 0 as ::core::ffi::c_int) {
        (*z).c += ret_0;
        let mut ret_1: ::core::ffi::c_int = in_grouping(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            251 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_1 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_1;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
            let mut ret_2: ::core::ffi::c_int = out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                251 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_2 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_2;
                let mut ret_3: ::core::ffi::c_int = in_grouping(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    251 as ::core::ffi::c_int,
                    1 as ::core::ffi::c_int,
                );
                if !(ret_3 < 0 as ::core::ffi::c_int) {
                    (*z).c += ret_3;
                    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
                }
            }
        }
    }
    (*z).c = c3;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_postlude(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    loop {
        let mut c1: ::core::ffi::c_int = (*z).c;
        (*z).bra = (*z).c;
        if (*z).c >= (*z).l
            || *(*z).p.offset(((*z).c + 0 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                != 2 as ::core::ffi::c_int
            || 35652352 as ::core::ffi::c_int
                >> (*(*z).p.offset(((*z).c + 0 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                & 1 as ::core::ffi::c_int == 0
        {
            among_var = 7 as ::core::ffi::c_int;
        } else {
            among_var = find_among(
                z,
                &raw const a_1 as *const among,
                7 as ::core::ffi::c_int,
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
                let mut ret_1: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_10 as *const symbol,
                );
                if ret_1 < 0 as ::core::ffi::c_int {
                    return ret_1;
                }
            }
            4 => {
                let mut ret_2: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_11 as *const symbol,
                );
                if ret_2 < 0 as ::core::ffi::c_int {
                    return ret_2;
                }
            }
            5 => {
                let mut ret_3: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_12 as *const symbol,
                );
                if ret_3 < 0 as ::core::ffi::c_int {
                    return ret_3;
                }
            }
            6 => {
                let mut ret_4: ::core::ffi::c_int = slice_del(z);
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
            }
            7 => {
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
unsafe fn r_standard_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_4 as *const among,
        43 as ::core::ffi::c_int,
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
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_13 as *const symbol) == 0
            {
                (*z).c = (*z).l - m1;
            } else {
                (*z).bra = (*z).c;
                let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_3: ::core::ffi::c_int = r_R2(z);
                if ret_3 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m2;
                    let mut ret_5: ::core::ffi::c_int = slice_from_s(
                        z,
                        3 as ::core::ffi::c_int,
                        &raw const s_14 as *const symbol,
                    );
                    if ret_5 < 0 as ::core::ffi::c_int {
                        return ret_5;
                    }
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
            let mut ret_6: ::core::ffi::c_int = r_R2(z);
            if ret_6 <= 0 as ::core::ffi::c_int {
                return ret_6;
            }
            let mut ret_7: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_15 as *const symbol,
            );
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
        }
        4 => {
            let mut ret_8: ::core::ffi::c_int = r_R2(z);
            if ret_8 <= 0 as ::core::ffi::c_int {
                return ret_8;
            }
            let mut ret_9: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_16 as *const symbol,
            );
            if ret_9 < 0 as ::core::ffi::c_int {
                return ret_9;
            }
        }
        5 => {
            let mut ret_10: ::core::ffi::c_int = r_R2(z);
            if ret_10 <= 0 as ::core::ffi::c_int {
                return ret_10;
            }
            let mut ret_11: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_17 as *const symbol,
            );
            if ret_11 < 0 as ::core::ffi::c_int {
                return ret_11;
            }
        }
        6 => {
            let mut ret_12: ::core::ffi::c_int = r_RV(z);
            if ret_12 <= 0 as ::core::ffi::c_int {
                return ret_12;
            }
            let mut ret_13: ::core::ffi::c_int = slice_del(z);
            if ret_13 < 0 as ::core::ffi::c_int {
                return ret_13;
            }
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            among_var = find_among_b(
                z,
                &raw const a_2 as *const among,
                6 as ::core::ffi::c_int,
            );
            if among_var == 0 {
                (*z).c = (*z).l - m3;
            } else {
                (*z).bra = (*z).c;
                match among_var {
                    1 => {
                        current_block = 14001958660280927786;
                        match current_block {
                            4804377075063615140 => {
                                let mut ret_24: ::core::ffi::c_int = r_RV(z);
                                if ret_24 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_24 < 0 as ::core::ffi::c_int {
                                        return ret_24;
                                    }
                                    let mut ret_25: ::core::ffi::c_int = slice_from_s(
                                        z,
                                        1 as ::core::ffi::c_int,
                                        &raw const s_20 as *const symbol,
                                    );
                                    if ret_25 < 0 as ::core::ffi::c_int {
                                        return ret_25;
                                    }
                                }
                            }
                            2472048668343472511 => {
                                let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                                let mut ret_18: ::core::ffi::c_int = r_R2(z);
                                if ret_18 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m4;
                                    let mut ret_20: ::core::ffi::c_int = r_R1(z);
                                    if ret_20 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m3;
                                    } else {
                                        if ret_20 < 0 as ::core::ffi::c_int {
                                            return ret_20;
                                        }
                                        let mut ret_21: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_19 as *const symbol,
                                        );
                                        if ret_21 < 0 as ::core::ffi::c_int {
                                            return ret_21;
                                        }
                                    }
                                } else {
                                    if ret_18 < 0 as ::core::ffi::c_int {
                                        return ret_18;
                                    }
                                    let mut ret_19: ::core::ffi::c_int = slice_del(z);
                                    if ret_19 < 0 as ::core::ffi::c_int {
                                        return ret_19;
                                    }
                                }
                            }
                            14001958660280927786 => {
                                let mut ret_14: ::core::ffi::c_int = r_R2(z);
                                if ret_14 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                    let mut ret_15: ::core::ffi::c_int = slice_del(z);
                                    if ret_15 < 0 as ::core::ffi::c_int {
                                        return ret_15;
                                    }
                                    (*z).ket = (*z).c;
                                    if eq_s_b(
                                        z,
                                        2 as ::core::ffi::c_int,
                                        &raw const s_18 as *const symbol,
                                    ) == 0
                                    {
                                        (*z).c = (*z).l - m3;
                                    } else {
                                        (*z).bra = (*z).c;
                                        let mut ret_16: ::core::ffi::c_int = r_R2(z);
                                        if ret_16 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m3;
                                        } else {
                                            if ret_16 < 0 as ::core::ffi::c_int {
                                                return ret_16;
                                            }
                                            let mut ret_17: ::core::ffi::c_int = slice_del(z);
                                            if ret_17 < 0 as ::core::ffi::c_int {
                                                return ret_17;
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                let mut ret_22: ::core::ffi::c_int = r_R2(z);
                                if ret_22 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_22 < 0 as ::core::ffi::c_int {
                                        return ret_22;
                                    }
                                    let mut ret_23: ::core::ffi::c_int = slice_del(z);
                                    if ret_23 < 0 as ::core::ffi::c_int {
                                        return ret_23;
                                    }
                                }
                            }
                        }
                    }
                    2 => {
                        current_block = 2472048668343472511;
                        match current_block {
                            4804377075063615140 => {
                                let mut ret_24: ::core::ffi::c_int = r_RV(z);
                                if ret_24 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_24 < 0 as ::core::ffi::c_int {
                                        return ret_24;
                                    }
                                    let mut ret_25: ::core::ffi::c_int = slice_from_s(
                                        z,
                                        1 as ::core::ffi::c_int,
                                        &raw const s_20 as *const symbol,
                                    );
                                    if ret_25 < 0 as ::core::ffi::c_int {
                                        return ret_25;
                                    }
                                }
                            }
                            2472048668343472511 => {
                                let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                                let mut ret_18: ::core::ffi::c_int = r_R2(z);
                                if ret_18 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m4;
                                    let mut ret_20: ::core::ffi::c_int = r_R1(z);
                                    if ret_20 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m3;
                                    } else {
                                        if ret_20 < 0 as ::core::ffi::c_int {
                                            return ret_20;
                                        }
                                        let mut ret_21: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_19 as *const symbol,
                                        );
                                        if ret_21 < 0 as ::core::ffi::c_int {
                                            return ret_21;
                                        }
                                    }
                                } else {
                                    if ret_18 < 0 as ::core::ffi::c_int {
                                        return ret_18;
                                    }
                                    let mut ret_19: ::core::ffi::c_int = slice_del(z);
                                    if ret_19 < 0 as ::core::ffi::c_int {
                                        return ret_19;
                                    }
                                }
                            }
                            14001958660280927786 => {
                                let mut ret_14: ::core::ffi::c_int = r_R2(z);
                                if ret_14 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                    let mut ret_15: ::core::ffi::c_int = slice_del(z);
                                    if ret_15 < 0 as ::core::ffi::c_int {
                                        return ret_15;
                                    }
                                    (*z).ket = (*z).c;
                                    if eq_s_b(
                                        z,
                                        2 as ::core::ffi::c_int,
                                        &raw const s_18 as *const symbol,
                                    ) == 0
                                    {
                                        (*z).c = (*z).l - m3;
                                    } else {
                                        (*z).bra = (*z).c;
                                        let mut ret_16: ::core::ffi::c_int = r_R2(z);
                                        if ret_16 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m3;
                                        } else {
                                            if ret_16 < 0 as ::core::ffi::c_int {
                                                return ret_16;
                                            }
                                            let mut ret_17: ::core::ffi::c_int = slice_del(z);
                                            if ret_17 < 0 as ::core::ffi::c_int {
                                                return ret_17;
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                let mut ret_22: ::core::ffi::c_int = r_R2(z);
                                if ret_22 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_22 < 0 as ::core::ffi::c_int {
                                        return ret_22;
                                    }
                                    let mut ret_23: ::core::ffi::c_int = slice_del(z);
                                    if ret_23 < 0 as ::core::ffi::c_int {
                                        return ret_23;
                                    }
                                }
                            }
                        }
                    }
                    3 => {
                        current_block = 13349765058737954042;
                        match current_block {
                            4804377075063615140 => {
                                let mut ret_24: ::core::ffi::c_int = r_RV(z);
                                if ret_24 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_24 < 0 as ::core::ffi::c_int {
                                        return ret_24;
                                    }
                                    let mut ret_25: ::core::ffi::c_int = slice_from_s(
                                        z,
                                        1 as ::core::ffi::c_int,
                                        &raw const s_20 as *const symbol,
                                    );
                                    if ret_25 < 0 as ::core::ffi::c_int {
                                        return ret_25;
                                    }
                                }
                            }
                            2472048668343472511 => {
                                let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                                let mut ret_18: ::core::ffi::c_int = r_R2(z);
                                if ret_18 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m4;
                                    let mut ret_20: ::core::ffi::c_int = r_R1(z);
                                    if ret_20 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m3;
                                    } else {
                                        if ret_20 < 0 as ::core::ffi::c_int {
                                            return ret_20;
                                        }
                                        let mut ret_21: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_19 as *const symbol,
                                        );
                                        if ret_21 < 0 as ::core::ffi::c_int {
                                            return ret_21;
                                        }
                                    }
                                } else {
                                    if ret_18 < 0 as ::core::ffi::c_int {
                                        return ret_18;
                                    }
                                    let mut ret_19: ::core::ffi::c_int = slice_del(z);
                                    if ret_19 < 0 as ::core::ffi::c_int {
                                        return ret_19;
                                    }
                                }
                            }
                            14001958660280927786 => {
                                let mut ret_14: ::core::ffi::c_int = r_R2(z);
                                if ret_14 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                    let mut ret_15: ::core::ffi::c_int = slice_del(z);
                                    if ret_15 < 0 as ::core::ffi::c_int {
                                        return ret_15;
                                    }
                                    (*z).ket = (*z).c;
                                    if eq_s_b(
                                        z,
                                        2 as ::core::ffi::c_int,
                                        &raw const s_18 as *const symbol,
                                    ) == 0
                                    {
                                        (*z).c = (*z).l - m3;
                                    } else {
                                        (*z).bra = (*z).c;
                                        let mut ret_16: ::core::ffi::c_int = r_R2(z);
                                        if ret_16 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m3;
                                        } else {
                                            if ret_16 < 0 as ::core::ffi::c_int {
                                                return ret_16;
                                            }
                                            let mut ret_17: ::core::ffi::c_int = slice_del(z);
                                            if ret_17 < 0 as ::core::ffi::c_int {
                                                return ret_17;
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                let mut ret_22: ::core::ffi::c_int = r_R2(z);
                                if ret_22 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_22 < 0 as ::core::ffi::c_int {
                                        return ret_22;
                                    }
                                    let mut ret_23: ::core::ffi::c_int = slice_del(z);
                                    if ret_23 < 0 as ::core::ffi::c_int {
                                        return ret_23;
                                    }
                                }
                            }
                        }
                    }
                    4 => {
                        current_block = 4804377075063615140;
                        match current_block {
                            4804377075063615140 => {
                                let mut ret_24: ::core::ffi::c_int = r_RV(z);
                                if ret_24 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_24 < 0 as ::core::ffi::c_int {
                                        return ret_24;
                                    }
                                    let mut ret_25: ::core::ffi::c_int = slice_from_s(
                                        z,
                                        1 as ::core::ffi::c_int,
                                        &raw const s_20 as *const symbol,
                                    );
                                    if ret_25 < 0 as ::core::ffi::c_int {
                                        return ret_25;
                                    }
                                }
                            }
                            2472048668343472511 => {
                                let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                                let mut ret_18: ::core::ffi::c_int = r_R2(z);
                                if ret_18 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m4;
                                    let mut ret_20: ::core::ffi::c_int = r_R1(z);
                                    if ret_20 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m3;
                                    } else {
                                        if ret_20 < 0 as ::core::ffi::c_int {
                                            return ret_20;
                                        }
                                        let mut ret_21: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_19 as *const symbol,
                                        );
                                        if ret_21 < 0 as ::core::ffi::c_int {
                                            return ret_21;
                                        }
                                    }
                                } else {
                                    if ret_18 < 0 as ::core::ffi::c_int {
                                        return ret_18;
                                    }
                                    let mut ret_19: ::core::ffi::c_int = slice_del(z);
                                    if ret_19 < 0 as ::core::ffi::c_int {
                                        return ret_19;
                                    }
                                }
                            }
                            14001958660280927786 => {
                                let mut ret_14: ::core::ffi::c_int = r_R2(z);
                                if ret_14 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                    let mut ret_15: ::core::ffi::c_int = slice_del(z);
                                    if ret_15 < 0 as ::core::ffi::c_int {
                                        return ret_15;
                                    }
                                    (*z).ket = (*z).c;
                                    if eq_s_b(
                                        z,
                                        2 as ::core::ffi::c_int,
                                        &raw const s_18 as *const symbol,
                                    ) == 0
                                    {
                                        (*z).c = (*z).l - m3;
                                    } else {
                                        (*z).bra = (*z).c;
                                        let mut ret_16: ::core::ffi::c_int = r_R2(z);
                                        if ret_16 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m3;
                                        } else {
                                            if ret_16 < 0 as ::core::ffi::c_int {
                                                return ret_16;
                                            }
                                            let mut ret_17: ::core::ffi::c_int = slice_del(z);
                                            if ret_17 < 0 as ::core::ffi::c_int {
                                                return ret_17;
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                let mut ret_22: ::core::ffi::c_int = r_R2(z);
                                if ret_22 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                } else {
                                    if ret_22 < 0 as ::core::ffi::c_int {
                                        return ret_22;
                                    }
                                    let mut ret_23: ::core::ffi::c_int = slice_del(z);
                                    if ret_23 < 0 as ::core::ffi::c_int {
                                        return ret_23;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        7 => {
            let mut ret_26: ::core::ffi::c_int = r_R2(z);
            if ret_26 <= 0 as ::core::ffi::c_int {
                return ret_26;
            }
            let mut ret_27: ::core::ffi::c_int = slice_del(z);
            if ret_27 < 0 as ::core::ffi::c_int {
                return ret_27;
            }
            let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
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
                (*z).c = (*z).l - m5;
            } else {
                among_var = find_among_b(
                    z,
                    &raw const a_3 as *const among,
                    3 as ::core::ffi::c_int,
                );
                if among_var == 0 {
                    (*z).c = (*z).l - m5;
                } else {
                    (*z).bra = (*z).c;
                    match among_var {
                        1 => {
                            current_block = 10468276026569382870;
                            match current_block {
                                10265166559088591044 => {
                                    let mut ret_34: ::core::ffi::c_int = r_R2(z);
                                    if ret_34 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m5;
                                    } else {
                                        if ret_34 < 0 as ::core::ffi::c_int {
                                            return ret_34;
                                        }
                                        let mut ret_35: ::core::ffi::c_int = slice_del(z);
                                        if ret_35 < 0 as ::core::ffi::c_int {
                                            return ret_35;
                                        }
                                    }
                                }
                                10468276026569382870 => {
                                    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                                    let mut ret_28: ::core::ffi::c_int = r_R2(z);
                                    if ret_28 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m6;
                                        let mut ret_30: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_21 as *const symbol,
                                        );
                                        if ret_30 < 0 as ::core::ffi::c_int {
                                            return ret_30;
                                        }
                                    } else {
                                        if ret_28 < 0 as ::core::ffi::c_int {
                                            return ret_28;
                                        }
                                        let mut ret_29: ::core::ffi::c_int = slice_del(z);
                                        if ret_29 < 0 as ::core::ffi::c_int {
                                            return ret_29;
                                        }
                                    }
                                }
                                _ => {
                                    let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                                    let mut ret_31: ::core::ffi::c_int = r_R2(z);
                                    if ret_31 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m7;
                                        let mut ret_33: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_22 as *const symbol,
                                        );
                                        if ret_33 < 0 as ::core::ffi::c_int {
                                            return ret_33;
                                        }
                                    } else {
                                        if ret_31 < 0 as ::core::ffi::c_int {
                                            return ret_31;
                                        }
                                        let mut ret_32: ::core::ffi::c_int = slice_del(z);
                                        if ret_32 < 0 as ::core::ffi::c_int {
                                            return ret_32;
                                        }
                                    }
                                }
                            }
                        }
                        2 => {
                            current_block = 16974974966130203269;
                            match current_block {
                                10265166559088591044 => {
                                    let mut ret_34: ::core::ffi::c_int = r_R2(z);
                                    if ret_34 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m5;
                                    } else {
                                        if ret_34 < 0 as ::core::ffi::c_int {
                                            return ret_34;
                                        }
                                        let mut ret_35: ::core::ffi::c_int = slice_del(z);
                                        if ret_35 < 0 as ::core::ffi::c_int {
                                            return ret_35;
                                        }
                                    }
                                }
                                10468276026569382870 => {
                                    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                                    let mut ret_28: ::core::ffi::c_int = r_R2(z);
                                    if ret_28 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m6;
                                        let mut ret_30: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_21 as *const symbol,
                                        );
                                        if ret_30 < 0 as ::core::ffi::c_int {
                                            return ret_30;
                                        }
                                    } else {
                                        if ret_28 < 0 as ::core::ffi::c_int {
                                            return ret_28;
                                        }
                                        let mut ret_29: ::core::ffi::c_int = slice_del(z);
                                        if ret_29 < 0 as ::core::ffi::c_int {
                                            return ret_29;
                                        }
                                    }
                                }
                                _ => {
                                    let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                                    let mut ret_31: ::core::ffi::c_int = r_R2(z);
                                    if ret_31 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m7;
                                        let mut ret_33: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_22 as *const symbol,
                                        );
                                        if ret_33 < 0 as ::core::ffi::c_int {
                                            return ret_33;
                                        }
                                    } else {
                                        if ret_31 < 0 as ::core::ffi::c_int {
                                            return ret_31;
                                        }
                                        let mut ret_32: ::core::ffi::c_int = slice_del(z);
                                        if ret_32 < 0 as ::core::ffi::c_int {
                                            return ret_32;
                                        }
                                    }
                                }
                            }
                        }
                        3 => {
                            current_block = 10265166559088591044;
                            match current_block {
                                10265166559088591044 => {
                                    let mut ret_34: ::core::ffi::c_int = r_R2(z);
                                    if ret_34 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m5;
                                    } else {
                                        if ret_34 < 0 as ::core::ffi::c_int {
                                            return ret_34;
                                        }
                                        let mut ret_35: ::core::ffi::c_int = slice_del(z);
                                        if ret_35 < 0 as ::core::ffi::c_int {
                                            return ret_35;
                                        }
                                    }
                                }
                                10468276026569382870 => {
                                    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                                    let mut ret_28: ::core::ffi::c_int = r_R2(z);
                                    if ret_28 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m6;
                                        let mut ret_30: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_21 as *const symbol,
                                        );
                                        if ret_30 < 0 as ::core::ffi::c_int {
                                            return ret_30;
                                        }
                                    } else {
                                        if ret_28 < 0 as ::core::ffi::c_int {
                                            return ret_28;
                                        }
                                        let mut ret_29: ::core::ffi::c_int = slice_del(z);
                                        if ret_29 < 0 as ::core::ffi::c_int {
                                            return ret_29;
                                        }
                                    }
                                }
                                _ => {
                                    let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                                    let mut ret_31: ::core::ffi::c_int = r_R2(z);
                                    if ret_31 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m7;
                                        let mut ret_33: ::core::ffi::c_int = slice_from_s(
                                            z,
                                            3 as ::core::ffi::c_int,
                                            &raw const s_22 as *const symbol,
                                        );
                                        if ret_33 < 0 as ::core::ffi::c_int {
                                            return ret_33;
                                        }
                                    } else {
                                        if ret_31 < 0 as ::core::ffi::c_int {
                                            return ret_31;
                                        }
                                        let mut ret_32: ::core::ffi::c_int = slice_del(z);
                                        if ret_32 < 0 as ::core::ffi::c_int {
                                            return ret_32;
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        8 => {
            let mut ret_36: ::core::ffi::c_int = r_R2(z);
            if ret_36 <= 0 as ::core::ffi::c_int {
                return ret_36;
            }
            let mut ret_37: ::core::ffi::c_int = slice_del(z);
            if ret_37 < 0 as ::core::ffi::c_int {
                return ret_37;
            }
            let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_23 as *const symbol) == 0
            {
                (*z).c = (*z).l - m8;
            } else {
                (*z).bra = (*z).c;
                let mut ret_38: ::core::ffi::c_int = r_R2(z);
                if ret_38 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m8;
                } else {
                    if ret_38 < 0 as ::core::ffi::c_int {
                        return ret_38;
                    }
                    let mut ret_39: ::core::ffi::c_int = slice_del(z);
                    if ret_39 < 0 as ::core::ffi::c_int {
                        return ret_39;
                    }
                    (*z).ket = (*z).c;
                    if eq_s_b(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_24 as *const symbol,
                    ) == 0
                    {
                        (*z).c = (*z).l - m8;
                    } else {
                        (*z).bra = (*z).c;
                        let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
                        let mut ret_40: ::core::ffi::c_int = r_R2(z);
                        if ret_40 == 0 as ::core::ffi::c_int {
                            (*z).c = (*z).l - m9;
                            let mut ret_42: ::core::ffi::c_int = slice_from_s(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_25 as *const symbol,
                            );
                            if ret_42 < 0 as ::core::ffi::c_int {
                                return ret_42;
                            }
                        } else {
                            if ret_40 < 0 as ::core::ffi::c_int {
                                return ret_40;
                            }
                            let mut ret_41: ::core::ffi::c_int = slice_del(z);
                            if ret_41 < 0 as ::core::ffi::c_int {
                                return ret_41;
                            }
                        }
                    }
                }
            }
        }
        9 => {
            let mut ret_43: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_26 as *const symbol,
            );
            if ret_43 < 0 as ::core::ffi::c_int {
                return ret_43;
            }
        }
        10 => {
            let mut ret_44: ::core::ffi::c_int = r_R1(z);
            if ret_44 <= 0 as ::core::ffi::c_int {
                return ret_44;
            }
            let mut ret_45: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_27 as *const symbol,
            );
            if ret_45 < 0 as ::core::ffi::c_int {
                return ret_45;
            }
        }
        11 => {
            let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_46: ::core::ffi::c_int = r_R2(z);
            if ret_46 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m10;
                let mut ret_48: ::core::ffi::c_int = r_R1(z);
                if ret_48 <= 0 as ::core::ffi::c_int {
                    return ret_48;
                }
                let mut ret_49: ::core::ffi::c_int = slice_from_s(
                    z,
                    3 as ::core::ffi::c_int,
                    &raw const s_28 as *const symbol,
                );
                if ret_49 < 0 as ::core::ffi::c_int {
                    return ret_49;
                }
            } else {
                if ret_46 < 0 as ::core::ffi::c_int {
                    return ret_46;
                }
                let mut ret_47: ::core::ffi::c_int = slice_del(z);
                if ret_47 < 0 as ::core::ffi::c_int {
                    return ret_47;
                }
            }
        }
        12 => {
            let mut ret_50: ::core::ffi::c_int = r_R1(z);
            if ret_50 <= 0 as ::core::ffi::c_int {
                return ret_50;
            }
            if out_grouping_b(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                251 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_51: ::core::ffi::c_int = slice_del(z);
            if ret_51 < 0 as ::core::ffi::c_int {
                return ret_51;
            }
        }
        13 => {
            let mut ret_52: ::core::ffi::c_int = r_RV(z);
            if ret_52 <= 0 as ::core::ffi::c_int {
                return ret_52;
            }
            let mut ret_53: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_29 as *const symbol,
            );
            if ret_53 < 0 as ::core::ffi::c_int {
                return ret_53;
            }
            return 0 as ::core::ffi::c_int;
        }
        14 => {
            let mut ret_54: ::core::ffi::c_int = r_RV(z);
            if ret_54 <= 0 as ::core::ffi::c_int {
                return ret_54;
            }
            let mut ret_55: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_30 as *const symbol,
            );
            if ret_55 < 0 as ::core::ffi::c_int {
                return ret_55;
            }
            return 0 as ::core::ffi::c_int;
        }
        15 => {
            let mut m_test11: ::core::ffi::c_int = (*z).l - (*z).c;
            if in_grouping_b(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                251 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_56: ::core::ffi::c_int = r_RV(z);
            if ret_56 <= 0 as ::core::ffi::c_int {
                return ret_56;
            }
            (*z).c = (*z).l - m_test11;
            let mut ret_57: ::core::ffi::c_int = slice_del(z);
            if ret_57 < 0 as ::core::ffi::c_int {
                return ret_57;
            }
            return 0 as ::core::ffi::c_int;
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_i_verb_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(2 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(2 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 68944418 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_5 as *const among, 35 as ::core::ffi::c_int) == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'H' as i32
    {
        (*z).c = (*z).l - m2;
    } else {
        (*z).c -= 1;
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    if out_grouping_b(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        251 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = mlimit1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_verb_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(2 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(2 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_6 as *const among,
        38 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = r_R2(z);
            if ret == 0 as ::core::ffi::c_int {
                (*z).lb = mlimit1;
                return 0 as ::core::ffi::c_int;
            }
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'e' as i32
            {
                (*z).c = (*z).l - m2;
            } else {
                (*z).c -= 1;
                (*z).bra = (*z).c;
                let mut ret_3: ::core::ffi::c_int = slice_del(z);
                if ret_3 < 0 as ::core::ffi::c_int {
                    return ret_3;
                }
            }
        }
        _ => {}
    }
    (*z).lb = mlimit1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_residual_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 's' as i32
    {
        (*z).c = (*z).l - m1;
    } else {
        (*z).c -= 1;
        (*z).bra = (*z).c;
        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
        if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_31 as *const symbol) == 0 {
            (*z).c = (*z).l - m3;
            if out_grouping_b(
                z,
                &raw const g_keep_with_s as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                232 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                (*z).c = (*z).l - m1;
                current_block = 4956146061682418353;
            } else {
                current_block = 18158521364840936293;
            }
        } else {
            current_block = 18158521364840936293;
        }
        match current_block {
            4956146061682418353 => {}
            _ => {
                (*z).c = (*z).l - m_test2;
                let mut ret: ::core::ffi::c_int = slice_del(z);
                if ret < 0 as ::core::ffi::c_int {
                    return ret;
                }
            }
        }
    }
    let mut mlimit4: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(2 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit4 = (*z).lb;
    (*z).lb = *(*z).I.offset(2 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 278560 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        (*z).lb = mlimit4;
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_7 as *const among, 6 as ::core::ffi::c_int);
    if among_var == 0 {
        (*z).lb = mlimit4;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = r_R2(z);
            if ret_0 == 0 as ::core::ffi::c_int {
                (*z).lb = mlimit4;
                return 0 as ::core::ffi::c_int;
            }
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
            let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 's' as i32
            {
                (*z).c = (*z).l - m5;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 't' as i32
                {
                    (*z).lb = mlimit4;
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
        2 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_32 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        3 => {
            let mut ret_3: ::core::ffi::c_int = slice_del(z);
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        _ => {}
    }
    (*z).lb = mlimit4;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_un_double(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m_test1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 1069056 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_8 as *const among, 5 as ::core::ffi::c_int) == 0 {
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
unsafe fn r_un_accent(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut i: ::core::ffi::c_int = 1 as ::core::ffi::c_int;
    while !(out_grouping_b(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        251 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0)
    {
        i -= 1;
    }
    if i > 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 0xe9 as ::core::ffi::c_int
    {
        (*z).c = (*z).l - m1;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 0xe8 as ::core::ffi::c_int
        {
            return 0 as ::core::ffi::c_int;
        }
        (*z).c -= 1;
    } else {
        (*z).c -= 1;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_from_s(
        z,
        1 as ::core::ffi::c_int,
        &raw const s_33 as *const symbol,
    );
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn french_ISO_8859_1_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_elisions(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
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
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_standard_suffix(z);
    if ret_2 == 0 as ::core::ffi::c_int {
        (*z).c = (*z).l - m6;
        let mut ret_3: ::core::ffi::c_int = r_i_verb_suffix(z);
        if ret_3 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m6;
            let mut ret_4: ::core::ffi::c_int = r_verb_suffix(z);
            if ret_4 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m4;
                let mut ret_7: ::core::ffi::c_int = r_residual_suffix(z);
                if ret_7 == 0 as ::core::ffi::c_int {
                    current_block = 11144331291078896904;
                } else {
                    if ret_7 < 0 as ::core::ffi::c_int {
                        return ret_7;
                    }
                    current_block = 11144331291078896904;
                }
            } else {
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
                current_block = 3784700663003424399;
            }
        } else {
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
            current_block = 3784700663003424399;
        }
    } else {
        if ret_2 < 0 as ::core::ffi::c_int {
            return ret_2;
        }
        current_block = 3784700663003424399;
    }
    match current_block {
        3784700663003424399 => {
            (*z).c = (*z).l - m5;
            let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'Y' as i32
            {
                (*z).c = (*z).l - m8;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 0xe7 as ::core::ffi::c_int
                {
                    (*z).c = (*z).l - m7;
                } else {
                    (*z).c -= 1;
                    (*z).bra = (*z).c;
                    let mut ret_6: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_35 as *const symbol,
                    );
                    if ret_6 < 0 as ::core::ffi::c_int {
                        return ret_6;
                    }
                }
            } else {
                (*z).c -= 1;
                (*z).bra = (*z).c;
                let mut ret_5: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_34 as *const symbol,
                );
                if ret_5 < 0 as ::core::ffi::c_int {
                    return ret_5;
                }
            }
        }
        _ => {}
    }
    (*z).c = (*z).l - m3;
    let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_8: ::core::ffi::c_int = r_un_double(z);
    if ret_8 < 0 as ::core::ffi::c_int {
        return ret_8;
    }
    (*z).c = (*z).l - m9;
    let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_9: ::core::ffi::c_int = r_un_accent(z);
    if ret_9 < 0 as ::core::ffi::c_int {
        return ret_9;
    }
    (*z).c = (*z).l - m10;
    (*z).c = (*z).lb;
    let mut c11: ::core::ffi::c_int = (*z).c;
    let mut ret_10: ::core::ffi::c_int = r_postlude(z);
    if ret_10 < 0 as ::core::ffi::c_int {
        return ret_10;
    }
    (*z).c = c11;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn french_ISO_8859_1_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 3 as ::core::ffi::c_int);
}
pub unsafe fn french_ISO_8859_1_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
