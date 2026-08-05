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
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_0_2: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_0_3: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
];
static mut s_0_4: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_0_5: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 6] = unsafe {
    [
        among {
            s_size: 0 as ::core::ffi::c_int,
            s: ::core::ptr::null::<symbol>(),
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 2] = ['l' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_1: [symbol; 4] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_2: [symbol; 2] = ['l' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_3: [symbol; 2] = ['m' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_4: [symbol; 2] = ['s' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_5: [symbol; 2] = ['l' as i32 as symbol, 'o' as i32 as symbol];
static mut s_1_6: [symbol; 4] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_7: [symbol; 3] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_8: [symbol; 5] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_9: [symbol; 3] = [
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_10: [symbol; 3] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_11: [symbol; 5] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_12: [symbol; 3] = [
    'n' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut a_1: [among; 13] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_1: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_2: [symbol; 5] = [
    'y' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_3: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_4: [symbol; 6] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_5: [symbol; 2] = ['a' as i32 as symbol, 'r' as i32 as symbol];
static mut s_2_6: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_2_7: [symbol; 2] = ['i' as i32 as symbol, 'r' as i32 as symbol];
static mut s_2_8: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
];
static mut s_2_9: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
];
static mut s_2_10: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
];
static mut a_2: [among; 11] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_3_1: [symbol; 2] = ['a' as i32 as symbol, 'd' as i32 as symbol];
static mut s_3_2: [symbol; 2] = ['o' as i32 as symbol, 's' as i32 as symbol];
static mut s_3_3: [symbol; 2] = ['i' as i32 as symbol, 'v' as i32 as symbol];
static mut a_3: [among; 4] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_1: [symbol; 4] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_4_2: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut a_4: [among; 3] = unsafe {
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
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
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
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_2: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_3: [symbol; 5] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_4: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_5: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_6: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_7: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_6_8: [symbol; 6] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_6_9: [symbol; 4] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
];
static mut s_6_10: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_11: [symbol; 4] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_12: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_13: [symbol; 5] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_14: [symbol; 6] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_6_15: [symbol; 6] = [
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_6_16: [symbol; 6] = [
    'u' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_6_17: [symbol; 3] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_18: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_19: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_20: [symbol; 7] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_21: [symbol; 7] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_22: [symbol; 3] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6_23: [symbol; 4] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_6_24: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_25: [symbol; 6] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_26: [symbol; 6] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_27: [symbol; 6] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_28: [symbol; 4] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_29: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_30: [symbol; 4] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_31: [symbol; 5] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_32: [symbol; 7] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_33: [symbol; 6] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_34: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_35: [symbol; 5] = [
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_36: [symbol; 7] = [
    'a' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_37: [symbol; 7] = [
    'u' as i32 as symbol,
    'c' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_38: [symbol; 6] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_39: [symbol; 5] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_40: [symbol; 4] = [
    'i' as i32 as symbol,
    'c' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_41: [symbol; 5] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_42: [symbol; 4] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_43: [symbol; 8] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_44: [symbol; 8] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6_45: [symbol; 4] = [
    'i' as i32 as symbol,
    'v' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut a_6: [among; 46] = unsafe {
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
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_14 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_6_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
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
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
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
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_6_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_6_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_6_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_6_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
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
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_6_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_6_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 2] = ['y' as i32 as symbol, 'a' as i32 as symbol];
static mut s_7_1: [symbol; 2] = ['y' as i32 as symbol, 'e' as i32 as symbol];
static mut s_7_2: [symbol; 3] = [
    'y' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_7_3: [symbol; 3] = [
    'y' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_7_4: [symbol; 5] = [
    'y' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_7_5: [symbol; 5] = [
    'y' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7_6: [symbol; 2] = ['y' as i32 as symbol, 'o' as i32 as symbol];
static mut s_7_7: [symbol; 3] = [
    'y' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_7_8: [symbol; 3] = [
    'y' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_7_9: [symbol; 4] = [
    'y' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_7_10: [symbol; 5] = [
    'y' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_7_11: [symbol; 3] = [
    'y' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut a_7: [among; 12] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_7_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_8_0: [symbol; 3] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_8_1: [symbol; 3] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_8_2: [symbol; 3] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_8_3: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_8_4: [symbol; 4] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_8_5: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_8_6: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_8_7: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_8_8: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
];
static mut s_8_9: [symbol; 2] = ['a' as i32 as symbol, 'd' as i32 as symbol];
static mut s_8_10: [symbol; 2] = ['e' as i32 as symbol, 'd' as i32 as symbol];
static mut s_8_11: [symbol; 2] = ['i' as i32 as symbol, 'd' as i32 as symbol];
static mut s_8_12: [symbol; 3] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_8_13: [symbol; 4] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_8_14: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_8_15: [symbol; 4] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_8_16: [symbol; 2] = ['a' as i32 as symbol, 'n' as i32 as symbol];
static mut s_8_17: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_18: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_19: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_20: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_21: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_22: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_23: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_24: [symbol; 2] = ['e' as i32 as symbol, 'n' as i32 as symbol];
static mut s_8_25: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_26: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_27: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_28: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_29: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_8_30: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_8_31: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_8_32: [symbol; 3] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_8_33: [symbol; 3] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_8_34: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_8_35: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_8_36: [symbol; 2] = ['a' as i32 as symbol, 'r' as i32 as symbol];
static mut s_8_37: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_8_38: [symbol; 2] = ['i' as i32 as symbol, 'r' as i32 as symbol];
static mut s_8_39: [symbol; 2] = ['a' as i32 as symbol, 's' as i32 as symbol];
static mut s_8_40: [symbol; 4] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_41: [symbol; 4] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_42: [symbol; 4] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_43: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_44: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_45: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_46: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_47: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_48: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_49: [symbol; 2] = ['e' as i32 as symbol, 's' as i32 as symbol];
static mut s_8_50: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_51: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_52: [symbol; 5] = [
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_53: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_54: [symbol; 6] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_55: [symbol; 5] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_56: [symbol; 7] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_57: [symbol; 7] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_58: [symbol; 7] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_59: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_60: [symbol; 6] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_61: [symbol; 6] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_62: [symbol; 6] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_63: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_64: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_65: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_66: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_67: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_68: [symbol; 4] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_69: [symbol; 4] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_70: [symbol; 4] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_71: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_72: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_73: [symbol; 8] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_74: [symbol; 6] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_75: [symbol; 8] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_76: [symbol; 8] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_77: [symbol; 8] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_78: [symbol; 4] = [
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_79: [symbol; 6] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_80: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_81: [symbol; 6] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_82: [symbol; 7] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_83: [symbol; 8] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_84: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_8_85: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_8_86: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_8_87: [symbol; 5] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_8_88: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_8_89: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_8_90: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_8_91: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_8_92: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_8_93: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_8_94: [symbol; 4] = [
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_8_95: [symbol; 3] = [
    'i' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut a_8: [among; 96] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_7 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_8 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_18 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_19 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_20 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_21 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_22 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_23 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_25 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_26 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_40 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_41 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_42 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_43 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_44 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_45 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_46 as *const symbol,
            substring_i: 45 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_47 as *const symbol,
            substring_i: 45 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_48 as *const symbol,
            substring_i: 45 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_8_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_50 as *const symbol,
            substring_i: 49 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_51 as *const symbol,
            substring_i: 49 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_53 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_55 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_8_56 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_8_57 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_8_58 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_59 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_61 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_62 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_63 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_64 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_65 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_66 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_67 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_68 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_69 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_70 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_8_71 as *const symbol,
            substring_i: 70 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_8_72 as *const symbol,
            substring_i: 70 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_8_73 as *const symbol,
            substring_i: 70 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_74 as *const symbol,
            substring_i: 70 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_8_75 as *const symbol,
            substring_i: 74 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_8_76 as *const symbol,
            substring_i: 74 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_8_77 as *const symbol,
            substring_i: 74 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_78 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_79 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_80 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_81 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_8_82 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_8_83 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_84 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_85 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_86 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_8_87 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_88 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_89 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_90 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_91 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_92 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_93 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_94 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_95 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_9_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_9_1: [symbol; 1] = ['e' as i32 as symbol];
static mut s_9_2: [symbol; 1] = ['o' as i32 as symbol];
static mut s_9_3: [symbol; 2] = ['o' as i32 as symbol, 's' as i32 as symbol];
static mut s_9_4: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_9_5: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_9_6: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
];
static mut s_9_7: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut a_9: [among; 8] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_9_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_9_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_9_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_9_7 as *const symbol,
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
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    4 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    10 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_1: [symbol; 1] = ['e' as i32 as symbol];
static mut s_2: [symbol; 1] = ['i' as i32 as symbol];
static mut s_3: [symbol; 1] = ['o' as i32 as symbol];
static mut s_4: [symbol; 1] = ['u' as i32 as symbol];
static mut s_5: [symbol; 5] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_6: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_7: [symbol; 2] = ['a' as i32 as symbol, 'r' as i32 as symbol];
static mut s_8: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_9: [symbol; 2] = ['i' as i32 as symbol, 'r' as i32 as symbol];
static mut s_10: [symbol; 2] = ['i' as i32 as symbol, 'c' as i32 as symbol];
static mut s_11: [symbol; 3] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_12: [symbol; 1] = ['u' as i32 as symbol];
static mut s_13: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_14: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_15: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
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
        252 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 18262830944043447035;
    } else {
        let mut c3: ::core::ffi::c_int = (*z).c;
        if out_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            252 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0
        {
            current_block = 8104907543545929955;
        } else {
            let mut ret: ::core::ffi::c_int = out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                252 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                current_block = 8104907543545929955;
            } else {
                (*z).c += ret;
                current_block = 4197718952895970926;
            }
        }
        match current_block {
            4197718952895970926 => {}
            _ => {
                (*z).c = c3;
                if in_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    252 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0
                {
                    current_block = 18262830944043447035;
                } else {
                    let mut ret_0: ::core::ffi::c_int = in_grouping_U(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        252 as ::core::ffi::c_int,
                        1 as ::core::ffi::c_int,
                    );
                    if ret_0 < 0 as ::core::ffi::c_int {
                        current_block = 18262830944043447035;
                    } else {
                        (*z).c += ret_0;
                        current_block = 4197718952895970926;
                    }
                }
            }
        }
    }
    match current_block {
        18262830944043447035 => {
            (*z).c = c2;
            if out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                252 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                current_block = 7368609826981629024;
            } else {
                let mut c4: ::core::ffi::c_int = (*z).c;
                if out_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    252 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0
                {
                    current_block = 6287503578918239547;
                } else {
                    let mut ret_1: ::core::ffi::c_int = out_grouping_U(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        252 as ::core::ffi::c_int,
                        1 as ::core::ffi::c_int,
                    );
                    if ret_1 < 0 as ::core::ffi::c_int {
                        current_block = 6287503578918239547;
                    } else {
                        (*z).c += ret_1;
                        current_block = 4197718952895970926;
                    }
                }
                match current_block {
                    4197718952895970926 => {}
                    _ => {
                        (*z).c = c4;
                        if in_grouping_U(
                            z,
                            &raw const g_v as *const ::core::ffi::c_uchar,
                            97 as ::core::ffi::c_int,
                            252 as ::core::ffi::c_int,
                            0 as ::core::ffi::c_int,
                        ) != 0
                        {
                            current_block = 7368609826981629024;
                        } else {
                            let mut ret_2: ::core::ffi::c_int = skip_utf8(
                                (*z).p,
                                (*z).c,
                                (*z).l,
                                1 as ::core::ffi::c_int,
                            );
                            if ret_2 < 0 as ::core::ffi::c_int {
                                current_block = 7368609826981629024;
                            } else {
                                (*z).c = ret_2;
                                current_block = 4197718952895970926;
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    match current_block {
        4197718952895970926 => {
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
        252 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret_3 < 0 as ::core::ffi::c_int) {
        (*z).c += ret_3;
        let mut ret_4: ::core::ffi::c_int = in_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            252 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_4 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_4;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
            let mut ret_5: ::core::ffi::c_int = out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                252 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_5 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_5;
                let mut ret_6: ::core::ffi::c_int = in_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    252 as ::core::ffi::c_int,
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
        if (*z).c + 1 as ::core::ffi::c_int >= (*z).l
            || *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                != 5 as ::core::ffi::c_int
            || 67641858 as ::core::ffi::c_int
                >> (*(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                & 1 as ::core::ffi::c_int == 0
        {
            among_var = 6 as ::core::ffi::c_int;
        } else {
            among_var = find_among(
                z,
                &raw const a_0 as *const among,
                6 as ::core::ffi::c_int,
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
                let mut ret_4: ::core::ffi::c_int = skip_utf8(
                    (*z).p,
                    (*z).c,
                    (*z).l,
                    1 as ::core::ffi::c_int,
                );
                if ret_4 < 0 as ::core::ffi::c_int {
                    (*z).c = c1;
                    break;
                } else {
                    (*z).c = ret_4;
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
        || 557090 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_1 as *const among, 13 as ::core::ffi::c_int) == 0 {
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
    among_var = find_among_b(
        z,
        &raw const a_2 as *const among,
        11 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = r_RV(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    match among_var {
        1 => {
            (*z).bra = (*z).c;
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_5 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            (*z).bra = (*z).c;
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_6 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            (*z).bra = (*z).c;
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        4 => {
            (*z).bra = (*z).c;
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_8 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        5 => {
            (*z).bra = (*z).c;
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_9 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        6 => {
            let mut ret_5: ::core::ffi::c_int = slice_del(z);
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        7 => {
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'u' as i32
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
            let mut ret_6: ::core::ffi::c_int = slice_del(z);
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_standard_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 835634 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_6 as *const among,
        46 as ::core::ffi::c_int,
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
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_10 as *const symbol) == 0
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
                &raw const s_11 as *const symbol,
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
                &raw const s_12 as *const symbol,
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
                &raw const s_13 as *const symbol,
            );
            if ret_10 < 0 as ::core::ffi::c_int {
                return ret_10;
            }
        }
        6 => {
            let mut ret_11: ::core::ffi::c_int = r_R1(z);
            if ret_11 <= 0 as ::core::ffi::c_int {
                return ret_11;
            }
            let mut ret_12: ::core::ffi::c_int = slice_del(z);
            if ret_12 < 0 as ::core::ffi::c_int {
                return ret_12;
            }
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                    != 3 as ::core::ffi::c_int
                || 4718616 as ::core::ffi::c_int
                    >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                    & 1 as ::core::ffi::c_int == 0
            {
                (*z).c = (*z).l - m2;
            } else {
                among_var = find_among_b(
                    z,
                    &raw const a_3 as *const among,
                    4 as ::core::ffi::c_int,
                );
                if among_var == 0 {
                    (*z).c = (*z).l - m2;
                } else {
                    (*z).bra = (*z).c;
                    let mut ret_13: ::core::ffi::c_int = r_R2(z);
                    if ret_13 == 0 as ::core::ffi::c_int {
                        (*z).c = (*z).l - m2;
                    } else {
                        if ret_13 < 0 as ::core::ffi::c_int {
                            return ret_13;
                        }
                        let mut ret_14: ::core::ffi::c_int = slice_del(z);
                        if ret_14 < 0 as ::core::ffi::c_int {
                            return ret_14;
                        }
                        match among_var {
                            1 => {
                                (*z).ket = (*z).c;
                                if eq_s_b(
                                    z,
                                    2 as ::core::ffi::c_int,
                                    &raw const s_14 as *const symbol,
                                ) == 0
                                {
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
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        7 => {
            let mut ret_17: ::core::ffi::c_int = r_R2(z);
            if ret_17 <= 0 as ::core::ffi::c_int {
                return ret_17;
            }
            let mut ret_18: ::core::ffi::c_int = slice_del(z);
            if ret_18 < 0 as ::core::ffi::c_int {
                return ret_18;
            }
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 101 as ::core::ffi::c_int
            {
                (*z).c = (*z).l - m3;
            } else if find_among_b(
                z,
                &raw const a_4 as *const among,
                3 as ::core::ffi::c_int,
            ) == 0
            {
                (*z).c = (*z).l - m3;
            } else {
                (*z).bra = (*z).c;
                let mut ret_19: ::core::ffi::c_int = r_R2(z);
                if ret_19 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m3;
                } else {
                    if ret_19 < 0 as ::core::ffi::c_int {
                        return ret_19;
                    }
                    let mut ret_20: ::core::ffi::c_int = slice_del(z);
                    if ret_20 < 0 as ::core::ffi::c_int {
                        return ret_20;
                    }
                }
            }
        }
        8 => {
            let mut ret_21: ::core::ffi::c_int = r_R2(z);
            if ret_21 <= 0 as ::core::ffi::c_int {
                return ret_21;
            }
            let mut ret_22: ::core::ffi::c_int = slice_del(z);
            if ret_22 < 0 as ::core::ffi::c_int {
                return ret_22;
            }
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
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
                (*z).c = (*z).l - m4;
            } else if find_among_b(
                z,
                &raw const a_5 as *const among,
                3 as ::core::ffi::c_int,
            ) == 0
            {
                (*z).c = (*z).l - m4;
            } else {
                (*z).bra = (*z).c;
                let mut ret_23: ::core::ffi::c_int = r_R2(z);
                if ret_23 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m4;
                } else {
                    if ret_23 < 0 as ::core::ffi::c_int {
                        return ret_23;
                    }
                    let mut ret_24: ::core::ffi::c_int = slice_del(z);
                    if ret_24 < 0 as ::core::ffi::c_int {
                        return ret_24;
                    }
                }
            }
        }
        9 => {
            let mut ret_25: ::core::ffi::c_int = r_R2(z);
            if ret_25 <= 0 as ::core::ffi::c_int {
                return ret_25;
            }
            let mut ret_26: ::core::ffi::c_int = slice_del(z);
            if ret_26 < 0 as ::core::ffi::c_int {
                return ret_26;
            }
            let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_15 as *const symbol) == 0
            {
                (*z).c = (*z).l - m5;
            } else {
                (*z).bra = (*z).c;
                let mut ret_27: ::core::ffi::c_int = r_R2(z);
                if ret_27 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m5;
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
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_y_verb_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(2 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(2 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_7 as *const among, 12 as ::core::ffi::c_int) == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'u' as i32
    {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c -= 1;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
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
        &raw const a_8 as *const among,
        96 as ::core::ffi::c_int,
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
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'u' as i32
            {
                (*z).c = (*z).l - m2;
            } else {
                (*z).c -= 1;
                let mut m_test3: ::core::ffi::c_int = (*z).l - (*z).c;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 'g' as i32
                {
                    (*z).c = (*z).l - m2;
                } else {
                    (*z).c -= 1;
                    (*z).c = (*z).l - m_test3;
                }
            }
            (*z).bra = (*z).c;
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
unsafe fn r_residual_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, &raw const a_9 as *const among, 8 as ::core::ffi::c_int);
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
            let mut ret_1: ::core::ffi::c_int = r_RV(z);
            if ret_1 <= 0 as ::core::ffi::c_int {
                return ret_1;
            }
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'u' as i32
            {
                (*z).c = (*z).l - m1;
            } else {
                (*z).c -= 1;
                (*z).bra = (*z).c;
                let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 'g' as i32
                {
                    (*z).c = (*z).l - m1;
                } else {
                    (*z).c -= 1;
                    (*z).c = (*z).l - m_test2;
                    let mut ret_3: ::core::ffi::c_int = r_RV(z);
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
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn spanish_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
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
        let mut ret_2: ::core::ffi::c_int = r_y_verb_suffix(z);
        if ret_2 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m3;
            let mut ret_3: ::core::ffi::c_int = r_verb_suffix(z);
            if !(ret_3 == 0 as ::core::ffi::c_int) {
                if ret_3 < 0 as ::core::ffi::c_int {
                    return ret_3;
                }
            }
        } else if ret_2 < 0 as ::core::ffi::c_int {
            return ret_2
        }
    } else if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1
    }
    (*z).c = (*z).l - m2;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_4: ::core::ffi::c_int = r_residual_suffix(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    (*z).c = (*z).l - m4;
    (*z).c = (*z).lb;
    let mut c5: ::core::ffi::c_int = (*z).c;
    let mut ret_5: ::core::ffi::c_int = r_postlude(z);
    if ret_5 < 0 as ::core::ffi::c_int {
        return ret_5;
    }
    (*z).c = c5;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn spanish_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 3 as ::core::ffi::c_int);
}
pub unsafe fn spanish_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
