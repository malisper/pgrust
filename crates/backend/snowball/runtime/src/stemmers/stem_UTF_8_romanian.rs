use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_0_1: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
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
static mut s_2_0: [symbol; 2] = ['e' as i32 as symbol, 'a' as i32 as symbol];
static mut s_2_1: [symbol; 5] = [
    'a' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_2: [symbol; 3] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_3: [symbol; 3] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_4: [symbol; 5] = [
    'a' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_5: [symbol; 3] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_6: [symbol; 3] = [
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_7: [symbol; 4] = [
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_8: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_9: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_10: [symbol; 2] = ['i' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_11: [symbol; 4] = [
    'u' as i32 as symbol,
    'l' as i32 as symbol,
    'u' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_2_12: [symbol; 2] = ['u' as i32 as symbol, 'l' as i32 as symbol];
static mut s_2_13: [symbol; 4] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_14: [symbol; 4] = [
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_2_15: [symbol; 5] = [
    'i' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut a_2: [among; 16] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
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
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_1: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_2: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_3: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_3_4: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_5: [symbol; 7] = [
    'a' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_6: [symbol; 7] = [
    'i' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_7: [symbol; 6] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_8: [symbol; 6] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_9: [symbol; 7] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_10: [symbol; 7] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_11: [symbol; 9] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_12: [symbol; 9] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_13: [symbol; 7] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_14: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_15: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_16: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_3_17: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_18: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_19: [symbol; 7] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_20: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_21: [symbol; 6] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_22: [symbol; 7] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_23: [symbol; 9] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_24: [symbol; 7] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_25: [symbol; 5] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_26: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_27: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_3_28: [symbol; 7] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_3_29: [symbol; 9] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_3_30: [symbol; 7] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_3_31: [symbol; 9] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_3_32: [symbol; 11] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_3_33: [symbol; 9] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_3_34: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_3_35: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_36: [symbol; 6] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_37: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_38: [symbol; 5] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_3_39: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
];
static mut s_3_40: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
];
static mut s_3_41: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
];
static mut s_3_42: [symbol; 6] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_3_43: [symbol; 6] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_3_44: [symbol; 6] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_3_45: [symbol; 6] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut a_3: [among; 46] = unsafe {
    [
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_3_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 11 as ::core::ffi::c_int,
            s: &raw const s_3_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_36 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_3_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_1: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_2: [symbol; 5] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_3: [symbol; 4] = [
    'o' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_4: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_5: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_6: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_7: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_8: [symbol; 3] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_9: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_4_10: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_4_11: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_12: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_13: [symbol; 5] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_14: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_15: [symbol; 4] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_16: [symbol; 4] = [
    'o' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_17: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_18: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_19: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_20: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_21: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_22: [symbol; 3] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_23: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_24: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_25: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_26: [symbol; 5] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_27: [symbol; 4] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_28: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_29: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_30: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_31: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_32: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_33: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_34: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_35: [symbol; 3] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_36: [symbol; 5] = [
    'i' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_37: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_4_38: [symbol; 5] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_4_39: [symbol; 4] = [
    'o' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_4_40: [symbol; 7] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_4_41: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_4_42: [symbol; 4] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_4_43: [symbol; 3] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_4_44: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_4_45: [symbol; 2] = ['o' as i32 as symbol, 's' as i32 as symbol];
static mut s_4_46: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_4_47: [symbol; 2] = ['i' as i32 as symbol, 't' as i32 as symbol];
static mut s_4_48: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_49: [symbol; 3] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_4_50: [symbol; 2] = ['u' as i32 as symbol, 't' as i32 as symbol];
static mut s_4_51: [symbol; 2] = ['i' as i32 as symbol, 'v' as i32 as symbol];
static mut s_4_52: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_53: [symbol; 6] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_54: [symbol; 6] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_55: [symbol; 5] = [
    'o' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_56: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_57: [symbol; 4] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_58: [symbol; 5] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_59: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_60: [symbol; 4] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_61: [symbol; 4] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 62] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
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
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_31 as *const symbol,
            substring_i: 30 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_4_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_46 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_47 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_50 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_51 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_53 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_55 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_56 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_57 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_58 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_4_59 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_61 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = ['e' as i32 as symbol, 'a' as i32 as symbol];
static mut s_5_1: [symbol; 2] = ['i' as i32 as symbol, 'a' as i32 as symbol];
static mut s_5_2: [symbol; 3] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
];
static mut s_5_3: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
];
static mut s_5_4: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_5_5: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_5_6: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_7: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_8: [symbol; 3] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_9: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_10: [symbol; 2] = ['s' as i32 as symbol, 'e' as i32 as symbol];
static mut s_5_11: [symbol; 3] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_12: [symbol; 4] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_13: [symbol; 3] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_14: [symbol; 3] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_15: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_16: [symbol; 5] = [
    'e' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_17: [symbol; 6] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_18: [symbol; 3] = [
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_5_19: [symbol; 2] = ['a' as i32 as symbol, 'i' as i32 as symbol];
static mut s_5_20: [symbol; 3] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_21: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_22: [symbol; 3] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_23: [symbol; 5] = [
    'e' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_24: [symbol; 6] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_25: [symbol; 2] = ['u' as i32 as symbol, 'i' as i32 as symbol];
static mut s_5_26: [symbol; 3] = [
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_5_27: [symbol; 4] = [
    'a' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_28: [symbol; 5] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_29: [symbol; 6] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_30: [symbol; 7] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_31: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_32: [symbol; 6] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_33: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_34: [symbol; 4] = [
    'i' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_35: [symbol; 4] = [
    'u' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_36: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_37: [symbol; 4] = [
    'a' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_38: [symbol; 5] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_39: [symbol; 5] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_40: [symbol; 4] = [
    'e' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_41: [symbol; 4] = [
    'i' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_42: [symbol; 7] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_43: [symbol; 8] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_44: [symbol; 9] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_45: [symbol; 10] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_46: [symbol; 9] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_47: [symbol; 9] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_48: [symbol; 10] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_49: [symbol; 7] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_50: [symbol; 7] = [
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_51: [symbol; 8] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_52: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_53: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_5_54: [symbol; 2] = ['a' as i32 as symbol, 'm' as i32 as symbol];
static mut s_5_55: [symbol; 3] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_5_56: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_5_57: [symbol; 2] = ['e' as i32 as symbol, 'm' as i32 as symbol];
static mut s_5_58: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_5_59: [symbol; 5] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_5_60: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_5_61: [symbol; 4] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_5_62: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_5_63: [symbol; 2] = ['i' as i32 as symbol, 'm' as i32 as symbol];
static mut s_5_64: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_65: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_66: [symbol; 6] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_67: [symbol; 7] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_68: [symbol; 8] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_69: [symbol; 7] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_70: [symbol; 7] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_71: [symbol; 8] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_72: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_73: [symbol; 5] = [
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_74: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_75: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_5_76: [symbol; 2] = ['a' as i32 as symbol, 'u' as i32 as symbol];
static mut s_5_77: [symbol; 3] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_5_78: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_5_79: [symbol; 4] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_5_80: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_5_81: [symbol; 2] = ['e' as i32 as symbol, 'z' as i32 as symbol];
static mut s_5_82: [symbol; 6] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'c' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_83: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_84: [symbol; 5] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_85: [symbol; 6] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_86: [symbol; 7] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_87: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_88: [symbol; 6] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_89: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_90: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_91: [symbol; 4] = [
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_92: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_93: [symbol; 5] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'z' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut a_5: [among; 94] = unsafe {
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
            s_size: 3 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_12 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_13 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_14 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_15 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_21 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_29 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_30 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_31 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_32 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_33 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_38 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_39 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_5_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_5_44 as *const symbol,
            substring_i: 43 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_5_45 as *const symbol,
            substring_i: 43 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_5_46 as *const symbol,
            substring_i: 43 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_5_47 as *const symbol,
            substring_i: 43 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_5_48 as *const symbol,
            substring_i: 43 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_50 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_5_51 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_53 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_55 as *const symbol,
            substring_i: 54 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_56 as *const symbol,
            substring_i: 54 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_57 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_58 as *const symbol,
            substring_i: 57 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_59 as *const symbol,
            substring_i: 57 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_60 as *const symbol,
            substring_i: 57 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_61 as *const symbol,
            substring_i: 57 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_62 as *const symbol,
            substring_i: 57 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_63 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_64 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_65 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_66 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_67 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_5_68 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_69 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_70 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_5_71 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_72 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_73 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_74 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_75 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_76 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_77 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_78 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_79 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_80 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_81 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_82 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_83 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_84 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_85 as *const symbol,
            substring_i: 84 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_86 as *const symbol,
            substring_i: 84 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_87 as *const symbol,
            substring_i: 84 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_88 as *const symbol,
            substring_i: 84 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_5_89 as *const symbol,
            substring_i: 84 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_90 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_91 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_92 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_5_93 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_6_1: [symbol; 1] = ['e' as i32 as symbol];
static mut s_6_2: [symbol; 2] = ['i' as i32 as symbol, 'e' as i32 as symbol];
static mut s_6_3: [symbol; 1] = ['i' as i32 as symbol];
static mut s_6_4: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut a_6: [among; 5] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 21] = [
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
    2 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    32 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    4 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 2] = [
    0xc8 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
];
static mut s_1: [symbol; 2] = [
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_2: [symbol; 1] = ['U' as i32 as symbol];
static mut s_3: [symbol; 1] = ['I' as i32 as symbol];
static mut s_4: [symbol; 1] = ['i' as i32 as symbol];
static mut s_5: [symbol; 1] = ['u' as i32 as symbol];
static mut s_6: [symbol; 1] = ['a' as i32 as symbol];
static mut s_7: [symbol; 1] = ['e' as i32 as symbol];
static mut s_8: [symbol; 1] = ['i' as i32 as symbol];
static mut s_9: [symbol; 2] = ['a' as i32 as symbol, 'b' as i32 as symbol];
static mut s_10: [symbol; 1] = ['i' as i32 as symbol];
static mut s_11: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_12: [symbol; 4] = [
    'a' as i32 as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_13: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_14: [symbol; 4] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_15: [symbol; 2] = ['i' as i32 as symbol, 'v' as i32 as symbol];
static mut s_16: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_17: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_18: [symbol; 2] = ['i' as i32 as symbol, 't' as i32 as symbol];
static mut s_19: [symbol; 2] = [
    0xc8 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_20: [symbol; 1] = ['t' as i32 as symbol];
static mut s_21: [symbol; 3] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
];
unsafe fn r_norm(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut c1: ::core::ffi::c_int = (*z).c;
    's_9: loop {
        let mut c2: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c3: ::core::ffi::c_int = (*z).c;
            (*z).bra = (*z).c;
            if !((*z).c + 1 as ::core::ffi::c_int >= (*z).l
                || *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 159 as ::core::ffi::c_int
                    && *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 163 as ::core::ffi::c_int)
            {
                among_var = find_among(
                    z,
                    &raw const a_0 as *const among,
                    2 as ::core::ffi::c_int,
                );
                if !(among_var == 0) {
                    (*z).ket = (*z).c;
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
                                2 as ::core::ffi::c_int,
                                &raw const s_1 as *const symbol,
                            );
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                        }
                        _ => {}
                    }
                    (*z).c = c3;
                    continue 's_9;
                }
            }
            (*z).c = c3;
            let mut ret_1: ::core::ffi::c_int = skip_utf8(
                (*z).p,
                (*z).c,
                (*z).l,
                1 as ::core::ffi::c_int,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                break;
            }
            (*z).c = ret_1;
        }
        (*z).c = c2;
        break;
    }
    (*z).c = c1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_prelude(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    's_4: loop {
        let mut c1: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c2: ::core::ffi::c_int = (*z).c;
            if !(in_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                259 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0)
            {
                (*z).bra = (*z).c;
                let mut c3: ::core::ffi::c_int = (*z).c;
                if (*z).c == (*z).l
                    || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                        != 'u' as i32
                {
                    current_block = 4382463662381651839;
                } else {
                    (*z).c += 1;
                    (*z).ket = (*z).c;
                    if in_grouping_U(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        259 as ::core::ffi::c_int,
                        0 as ::core::ffi::c_int,
                    ) != 0
                    {
                        current_block = 4382463662381651839;
                    } else {
                        let mut ret: ::core::ffi::c_int = slice_from_s(
                            z,
                            1 as ::core::ffi::c_int,
                            &raw const s_2 as *const symbol,
                        );
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 4149447384559459670;
                    }
                }
                match current_block {
                    4382463662381651839 => {
                        (*z).c = c3;
                        if (*z).c == (*z).l
                            || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                                != 'i' as i32
                        {
                            current_block = 7531562329915207461;
                        } else {
                            (*z).c += 1;
                            (*z).ket = (*z).c;
                            if in_grouping_U(
                                z,
                                &raw const g_v as *const ::core::ffi::c_uchar,
                                97 as ::core::ffi::c_int,
                                259 as ::core::ffi::c_int,
                                0 as ::core::ffi::c_int,
                            ) != 0
                            {
                                current_block = 7531562329915207461;
                            } else {
                                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    1 as ::core::ffi::c_int,
                                    &raw const s_3 as *const symbol,
                                );
                                if ret_0 < 0 as ::core::ffi::c_int {
                                    return ret_0;
                                }
                                current_block = 4149447384559459670;
                            }
                        }
                    }
                    _ => {}
                }
                match current_block {
                    7531562329915207461 => {}
                    _ => {
                        (*z).c = c2;
                        continue 's_4;
                    }
                }
            }
            (*z).c = c2;
            let mut ret_1: ::core::ffi::c_int = skip_utf8(
                (*z).p,
                (*z).c,
                (*z).l,
                1 as ::core::ffi::c_int,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                break;
            }
            (*z).c = ret_1;
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
    if in_grouping_U(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        259 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 3015432225250626078;
    } else {
        let mut c3: ::core::ffi::c_int = (*z).c;
        if out_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            259 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0
        {
            current_block = 5377118001061649667;
        } else {
            let mut ret: ::core::ffi::c_int = out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                259 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                current_block = 5377118001061649667;
            } else {
                (*z).c += ret;
                current_block = 11163006212697914241;
            }
        }
        match current_block {
            11163006212697914241 => {}
            _ => {
                (*z).c = c3;
                if in_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    259 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0
                {
                    current_block = 3015432225250626078;
                } else {
                    let mut ret_0: ::core::ffi::c_int = in_grouping_U(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        259 as ::core::ffi::c_int,
                        1 as ::core::ffi::c_int,
                    );
                    if ret_0 < 0 as ::core::ffi::c_int {
                        current_block = 3015432225250626078;
                    } else {
                        (*z).c += ret_0;
                        current_block = 11163006212697914241;
                    }
                }
            }
        }
    }
    match current_block {
        3015432225250626078 => {
            (*z).c = c2;
            if out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                259 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                current_block = 13645657685529746979;
            } else {
                let mut c4: ::core::ffi::c_int = (*z).c;
                if out_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    259 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0
                {
                    current_block = 5470322507484958751;
                } else {
                    let mut ret_1: ::core::ffi::c_int = out_grouping_U(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        259 as ::core::ffi::c_int,
                        1 as ::core::ffi::c_int,
                    );
                    if ret_1 < 0 as ::core::ffi::c_int {
                        current_block = 5470322507484958751;
                    } else {
                        (*z).c += ret_1;
                        current_block = 11163006212697914241;
                    }
                }
                match current_block {
                    11163006212697914241 => {}
                    _ => {
                        (*z).c = c4;
                        if in_grouping_U(
                            z,
                            &raw const g_v as *const ::core::ffi::c_uchar,
                            97 as ::core::ffi::c_int,
                            259 as ::core::ffi::c_int,
                            0 as ::core::ffi::c_int,
                        ) != 0
                        {
                            current_block = 13645657685529746979;
                        } else {
                            let mut ret_2: ::core::ffi::c_int = skip_utf8(
                                (*z).p,
                                (*z).c,
                                (*z).l,
                                1 as ::core::ffi::c_int,
                            );
                            if ret_2 < 0 as ::core::ffi::c_int {
                                current_block = 13645657685529746979;
                            } else {
                                (*z).c = ret_2;
                                current_block = 11163006212697914241;
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    match current_block {
        11163006212697914241 => {
            *(*z).I.offset(2 as ::core::ffi::c_int as isize) = (*z).c;
        }
        _ => {}
    }
    (*z).c = c1;
    let mut c5: ::core::ffi::c_int = (*z).c;
    let mut ret_3: ::core::ffi::c_int = out_grouping_U(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        259 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret_3 < 0 as ::core::ffi::c_int) {
        (*z).c += ret_3;
        let mut ret_4: ::core::ffi::c_int = in_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            259 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_4 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_4;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
            let mut ret_5: ::core::ffi::c_int = out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                259 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_5 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_5;
                let mut ret_6: ::core::ffi::c_int = in_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    259 as ::core::ffi::c_int,
                    1 as ::core::ffi::c_int,
                );
                if !(ret_6 < 0 as ::core::ffi::c_int) {
                    (*z).c += ret_6;
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
                    &raw const s_4 as *const symbol,
                );
                if ret < 0 as ::core::ffi::c_int {
                    return ret;
                }
            }
            2 => {
                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_5 as *const symbol,
                );
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
            }
            3 => {
                let mut ret_1: ::core::ffi::c_int = skip_utf8(
                    (*z).p,
                    (*z).c,
                    (*z).l,
                    1 as ::core::ffi::c_int,
                );
                if ret_1 < 0 as ::core::ffi::c_int {
                    (*z).c = c1;
                    break;
                } else {
                    (*z).c = ret_1;
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
unsafe fn r_step_0(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 266786 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_2 as *const among,
        16 as ::core::ffi::c_int,
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
                &raw const s_6 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        4 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_8 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        5 => {
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_9 as *const symbol) == 0 {
                (*z).c = (*z).l - m1;
            } else {
                return 0 as ::core::ffi::c_int
            }
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_10 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        6 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_11 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        7 => {
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_12 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_combo_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut m_test1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_3 as *const among,
        46 as ::core::ffi::c_int,
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
                &raw const s_13 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_14 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_15 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        4 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_16 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        5 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_17 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        6 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_18 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        _ => {}
    }
    *(*z).I.offset(3 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    (*z).c = (*z).l - m_test1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_standard_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    *(*z).I.offset(3 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    loop {
        let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret: ::core::ffi::c_int = r_combo_suffix(z);
        if ret == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m1;
            break;
        } else if ret < 0 as ::core::ffi::c_int {
            return ret
        }
    }
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_4 as *const among,
        62 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_R2(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    match among_var {
        1 => {
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        2 => {
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_19 as *const symbol) == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).bra = (*z).c;
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_20 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        3 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_21 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        _ => {}
    }
    *(*z).I.offset(3 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
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
        &raw const a_5 as *const among,
        94 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            if out_grouping_b_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                259 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                (*z).c = (*z).l - m2;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 'u' as i32
                {
                    (*z).lb = mlimit1;
                    return 0 as ::core::ffi::c_int;
                }
                (*z).c -= 1;
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
    (*z).lb = mlimit1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_vowel_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_6 as *const among, 5 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_RV(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn romanian_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_norm(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_prelude(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = c1;
    let mut ret_1: ::core::ffi::c_int = r_mark_regions(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_step_0(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_standard_suffix(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = (*z).l - m3;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    if *(*z).I.offset(3 as ::core::ffi::c_int as isize) == 0 {
        (*z).c = (*z).l - m5;
        let mut ret_4: ::core::ffi::c_int = r_verb_suffix(z);
        if !(ret_4 == 0 as ::core::ffi::c_int) {
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
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
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn romanian_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 4 as ::core::ffi::c_int);
}
pub unsafe fn romanian_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
