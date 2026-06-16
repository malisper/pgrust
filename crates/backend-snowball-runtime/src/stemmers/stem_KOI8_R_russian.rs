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
    0xd7 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_0_1: [symbol; 4] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_0_2: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_0_3: [symbol; 1] = [0xd7 as ::core::ffi::c_int as symbol];
static mut s_0_4: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
];
static mut s_0_5: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
];
static mut s_0_6: [symbol; 5] = [
    0xd7 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
    0xd3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
static mut s_0_7: [symbol; 6] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
    0xd3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
static mut s_0_8: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
    0xd3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 9] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 2] = [
    0xc0 as ::core::ffi::c_int as symbol,
    0xc0 as ::core::ffi::c_int as symbol,
];
static mut s_1_1: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xc0 as ::core::ffi::c_int as symbol,
];
static mut s_1_2: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xc0 as ::core::ffi::c_int as symbol,
];
static mut s_1_3: [symbol; 2] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xc0 as ::core::ffi::c_int as symbol,
];
static mut s_1_4: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_1_5: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_1_6: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_1_7: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_1_8: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
];
static mut s_1_9: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
];
static mut s_1_10: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_1_11: [symbol; 3] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_1_12: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_1_13: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_1_14: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_1_15: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_1_16: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_1_17: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_1_18: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_1_19: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_1_20: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xc7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
];
static mut s_1_21: [symbol; 3] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xc7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
];
static mut s_1_22: [symbol; 2] = [
    0xc1 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
];
static mut s_1_23: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
];
static mut s_1_24: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
];
static mut s_1_25: [symbol; 3] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 26] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_2_1: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
];
static mut s_2_2: [symbol; 2] = [
    0xd7 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
];
static mut s_2_3: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
];
static mut s_2_4: [symbol; 3] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
];
static mut s_2_5: [symbol; 1] = [0xdd as ::core::ffi::c_int as symbol];
static mut s_2_6: [symbol; 2] = [
    0xc0 as ::core::ffi::c_int as symbol,
    0xdd as ::core::ffi::c_int as symbol,
];
static mut s_2_7: [symbol; 3] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xc0 as ::core::ffi::c_int as symbol,
    0xdd as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 8] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 2] = [
    0xd3 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
];
static mut s_3_1: [symbol; 2] = [
    0xd3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
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
static mut s_4_0: [symbol; 1] = [0xc0 as ::core::ffi::c_int as symbol];
static mut s_4_1: [symbol; 2] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xc0 as ::core::ffi::c_int as symbol,
];
static mut s_4_2: [symbol; 2] = [
    0xcc as ::core::ffi::c_int as symbol,
    0xc1 as ::core::ffi::c_int as symbol,
];
static mut s_4_3: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xcc as ::core::ffi::c_int as symbol,
    0xc1 as ::core::ffi::c_int as symbol,
];
static mut s_4_4: [symbol; 3] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xcc as ::core::ffi::c_int as symbol,
    0xc1 as ::core::ffi::c_int as symbol,
];
static mut s_4_5: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xc1 as ::core::ffi::c_int as symbol,
];
static mut s_4_6: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xc1 as ::core::ffi::c_int as symbol,
];
static mut s_4_7: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_4_8: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_4_9: [symbol; 3] = [
    0xca as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_4_10: [symbol; 4] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_4_11: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_4_12: [symbol; 2] = [
    0xcc as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_4_13: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xcc as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_4_14: [symbol; 3] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xcc as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_4_15: [symbol; 1] = [0xca as ::core::ffi::c_int as symbol];
static mut s_4_16: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_4_17: [symbol; 2] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_4_18: [symbol; 1] = [0xcc as ::core::ffi::c_int as symbol];
static mut s_4_19: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xcc as ::core::ffi::c_int as symbol,
];
static mut s_4_20: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xcc as ::core::ffi::c_int as symbol,
];
static mut s_4_21: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_4_22: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_4_23: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_4_24: [symbol; 1] = [0xce as ::core::ffi::c_int as symbol];
static mut s_4_25: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
];
static mut s_4_26: [symbol; 2] = [
    0xcc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
];
static mut s_4_27: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xcc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
];
static mut s_4_28: [symbol; 3] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xcc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
];
static mut s_4_29: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
];
static mut s_4_30: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
];
static mut s_4_31: [symbol; 3] = [
    0xce as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
];
static mut s_4_32: [symbol; 2] = [
    0xc0 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
];
static mut s_4_33: [symbol; 3] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xc0 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
];
static mut s_4_34: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
];
static mut s_4_35: [symbol; 3] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
];
static mut s_4_36: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
];
static mut s_4_37: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
];
static mut s_4_38: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
];
static mut s_4_39: [symbol; 2] = [
    0xd4 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
static mut s_4_40: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
static mut s_4_41: [symbol; 3] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
static mut s_4_42: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
static mut s_4_43: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
static mut s_4_44: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
];
static mut s_4_45: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 46] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_11 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_14 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_17 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_20 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_25 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_28 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_31 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_33 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_35 as *const symbol,
            substring_i: 34 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_40 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_41 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_45 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 1] = [0xc0 as ::core::ffi::c_int as symbol];
static mut s_5_1: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xc0 as ::core::ffi::c_int as symbol,
];
static mut s_5_2: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xc0 as ::core::ffi::c_int as symbol,
];
static mut s_5_3: [symbol; 1] = [0xc1 as ::core::ffi::c_int as symbol];
static mut s_5_4: [symbol; 1] = [0xc5 as ::core::ffi::c_int as symbol];
static mut s_5_5: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_5_6: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_5_7: [symbol; 2] = [
    0xc1 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
];
static mut s_5_8: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
];
static mut s_5_9: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0xc8 as ::core::ffi::c_int as symbol,
];
static mut s_5_10: [symbol; 1] = [0xc9 as ::core::ffi::c_int as symbol];
static mut s_5_11: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_5_12: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_5_13: [symbol; 3] = [
    0xc1 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_5_14: [symbol; 3] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_5_15: [symbol; 4] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
    0xc9 as ::core::ffi::c_int as symbol,
];
static mut s_5_16: [symbol; 1] = [0xca as ::core::ffi::c_int as symbol];
static mut s_5_17: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_5_18: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_5_19: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_5_20: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
];
static mut s_5_21: [symbol; 2] = [
    0xc1 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_5_22: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_5_23: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_5_24: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_5_25: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_5_26: [symbol; 3] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0xcd as ::core::ffi::c_int as symbol,
];
static mut s_5_27: [symbol; 1] = [0xcf as ::core::ffi::c_int as symbol];
static mut s_5_28: [symbol; 1] = [0xd1 as ::core::ffi::c_int as symbol];
static mut s_5_29: [symbol; 2] = [
    0xc9 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
];
static mut s_5_30: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
];
static mut s_5_31: [symbol; 1] = [0xd5 as ::core::ffi::c_int as symbol];
static mut s_5_32: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
];
static mut s_5_33: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xd7 as ::core::ffi::c_int as symbol,
];
static mut s_5_34: [symbol; 1] = [0xd8 as ::core::ffi::c_int as symbol];
static mut s_5_35: [symbol; 1] = [0xd9 as ::core::ffi::c_int as symbol];
static mut a_5: [among; 36] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_6 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_12 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
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
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_19 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_20 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_29 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_30 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_5_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 3] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xd3 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
];
static mut s_6_1: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0xd3 as ::core::ffi::c_int as symbol,
    0xd4 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
];
static mut a_6: [among; 2] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 4] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
];
static mut s_7_1: [symbol; 1] = [0xce as ::core::ffi::c_int as symbol];
static mut s_7_2: [symbol; 1] = [0xd8 as ::core::ffi::c_int as symbol];
static mut s_7_3: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xca as ::core::ffi::c_int as symbol,
    0xdb as ::core::ffi::c_int as symbol,
];
static mut a_7: [among; 4] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_7_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 4] = [
    35 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    130 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    34 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    18 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 1] = [0xc5 as ::core::ffi::c_int as symbol];
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = out_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        192 as ::core::ffi::c_int,
        220 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret < 0 as ::core::ffi::c_int) {
        (*z).c += ret;
        *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
        let mut ret_0: ::core::ffi::c_int = in_grouping(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            192 as ::core::ffi::c_int,
            220 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_0 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_0;
            let mut ret_1: ::core::ffi::c_int = out_grouping(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                192 as ::core::ffi::c_int,
                220 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_1 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_1;
                let mut ret_2: ::core::ffi::c_int = in_grouping(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    192 as ::core::ffi::c_int,
                    220 as ::core::ffi::c_int,
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
unsafe fn r_R2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(0 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_perfective_gerund(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 6 as ::core::ffi::c_int
        || 25166336 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_0 as *const among, 9 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 0xc1 as ::core::ffi::c_int
            {
                (*z).c = (*z).l - m1;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 0xd1 as ::core::ffi::c_int
                {
                    return 0 as ::core::ffi::c_int;
                }
                (*z).c -= 1;
            } else {
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
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_adjective(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 6 as ::core::ffi::c_int
        || 2271009 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_1 as *const among, 26 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_adjectival(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    let mut ret: ::core::ffi::c_int = r_adjective(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 6 as ::core::ffi::c_int
        || 671113216 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        (*z).c = (*z).l - m1;
    } else {
        among_var = find_among_b(
            z,
            &raw const a_2 as *const among,
            8 as ::core::ffi::c_int,
        );
        if among_var == 0 {
            (*z).c = (*z).l - m1;
        } else {
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    current_block = 2868539653012386629;
                    match current_block {
                        14730262267959436050 => {
                            let mut ret_1: ::core::ffi::c_int = slice_del(z);
                            if ret_1 < 0 as ::core::ffi::c_int {
                                return ret_1;
                            }
                        }
                        _ => {
                            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
                            if (*z).c <= (*z).lb
                                || *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 0xc1 as ::core::ffi::c_int
                            {
                                (*z).c = (*z).l - m2;
                                if (*z).c <= (*z).lb
                                    || *(*z)
                                        .p
                                        .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                        as ::core::ffi::c_int != 0xd1 as ::core::ffi::c_int
                                {
                                    (*z).c = (*z).l - m1;
                                    current_block = 7056779235015430508;
                                } else {
                                    (*z).c -= 1;
                                    current_block = 2208184793441573377;
                                }
                            } else {
                                (*z).c -= 1;
                                current_block = 2208184793441573377;
                            }
                            match current_block {
                                7056779235015430508 => {}
                                _ => {
                                    let mut ret_0: ::core::ffi::c_int = slice_del(z);
                                    if ret_0 < 0 as ::core::ffi::c_int {
                                        return ret_0;
                                    }
                                }
                            }
                        }
                    }
                }
                2 => {
                    current_block = 14730262267959436050;
                    match current_block {
                        14730262267959436050 => {
                            let mut ret_1: ::core::ffi::c_int = slice_del(z);
                            if ret_1 < 0 as ::core::ffi::c_int {
                                return ret_1;
                            }
                        }
                        _ => {
                            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
                            if (*z).c <= (*z).lb
                                || *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 0xc1 as ::core::ffi::c_int
                            {
                                (*z).c = (*z).l - m2;
                                if (*z).c <= (*z).lb
                                    || *(*z)
                                        .p
                                        .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                        as ::core::ffi::c_int != 0xd1 as ::core::ffi::c_int
                                {
                                    (*z).c = (*z).l - m1;
                                    current_block = 7056779235015430508;
                                } else {
                                    (*z).c -= 1;
                                    current_block = 2208184793441573377;
                                }
                            } else {
                                (*z).c -= 1;
                                current_block = 2208184793441573377;
                            }
                            match current_block {
                                7056779235015430508 => {}
                                _ => {
                                    let mut ret_0: ::core::ffi::c_int = slice_del(z);
                                    if ret_0 < 0 as ::core::ffi::c_int {
                                        return ret_0;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_reflexive(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 209 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 216 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_3 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_verb(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 6 as ::core::ffi::c_int
        || 51443235 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_4 as *const among,
        46 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 0xc1 as ::core::ffi::c_int
            {
                (*z).c = (*z).l - m1;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 0xd1 as ::core::ffi::c_int
                {
                    return 0 as ::core::ffi::c_int;
                }
                (*z).c -= 1;
            } else {
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
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_noun(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 6 as ::core::ffi::c_int
        || 60991267 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_5 as *const among, 36 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_derivational(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 212 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 216 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_6 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = r_R2(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_tidy_up(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 6 as ::core::ffi::c_int
        || 151011360 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_7 as *const among, 4 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 0xce as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
            (*z).bra = (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 0xce as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 0xce as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c -= 1;
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
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn russian_KOI8_R_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut current_block_9: u64;
    loop {
        let mut c2: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c3: ::core::ffi::c_int = (*z).c;
            (*z).bra = (*z).c;
            if (*z).c == (*z).l
                || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int
                    != 0xa3 as ::core::ffi::c_int
            {
                (*z).c = c3;
                if (*z).c >= (*z).l {
                    current_block_9 = 9119711267172901778;
                    break;
                }
                (*z).c += 1;
            } else {
                (*z).c += 1;
                (*z).ket = (*z).c;
                (*z).c = c3;
                current_block_9 = 11812396948646013369;
                break;
            }
        }
        match current_block_9 {
            11812396948646013369 => {
                let mut ret: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_0 as *const symbol,
                );
                if ret < 0 as ::core::ffi::c_int {
                    return ret;
                }
            }
            _ => {
                (*z).c = c2;
                break;
            }
        }
    }
    (*z).c = c1;
    let mut ret_0: ::core::ffi::c_int = r_mark_regions(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut mlimit4: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit4 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_perfective_gerund(z);
    if ret_1 == 0 as ::core::ffi::c_int {
        (*z).c = (*z).l - m6;
        let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_2: ::core::ffi::c_int = r_reflexive(z);
        if ret_2 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m7;
        } else if ret_2 < 0 as ::core::ffi::c_int {
            return ret_2
        }
        let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_3: ::core::ffi::c_int = r_adjectival(z);
        if ret_3 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m8;
            let mut ret_4: ::core::ffi::c_int = r_verb(z);
            if ret_4 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m8;
                let mut ret_5: ::core::ffi::c_int = r_noun(z);
                if !(ret_5 == 0 as ::core::ffi::c_int) {
                    if ret_5 < 0 as ::core::ffi::c_int {
                        return ret_5;
                    }
                }
            } else if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4
            }
        } else if ret_3 < 0 as ::core::ffi::c_int {
            return ret_3
        }
    } else if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1
    }
    (*z).c = (*z).l - m5;
    let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 0xc9 as ::core::ffi::c_int
    {
        (*z).c = (*z).l - m9;
    } else {
        (*z).c -= 1;
        (*z).bra = (*z).c;
        let mut ret_6: ::core::ffi::c_int = slice_del(z);
        if ret_6 < 0 as ::core::ffi::c_int {
            return ret_6;
        }
    }
    let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_7: ::core::ffi::c_int = r_derivational(z);
    if ret_7 < 0 as ::core::ffi::c_int {
        return ret_7;
    }
    (*z).c = (*z).l - m10;
    let mut m11: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_8: ::core::ffi::c_int = r_tidy_up(z);
    if ret_8 < 0 as ::core::ffi::c_int {
        return ret_8;
    }
    (*z).c = (*z).l - m11;
    (*z).lb = mlimit4;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn russian_KOI8_R_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 2 as ::core::ffi::c_int);
}
pub unsafe fn russian_KOI8_R_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
