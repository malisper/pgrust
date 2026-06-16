use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_0_1: [symbol; 8] = [
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_0_2: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_0_3: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_0_4: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_0_5: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_0_6: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_0_7: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_8: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_9: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_10: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_11: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_12: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_13: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_14: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_15: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_16: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_17: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_18: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_0_19: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_0_20: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_0_21: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_0_22: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 23] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
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
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_12 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_16 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_1_1: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_1_2: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_1_3: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_1_4: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_1_5: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_1_6: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_1_7: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_1_8: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_1_9: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_1_10: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_1_11: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_1_12: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_1_13: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_1_14: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_15: [symbol; 6] = [
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_16: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_17: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_18: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_19: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_20: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_21: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_22: [symbol; 8] = [
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_23: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_24: [symbol; 12] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_25: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_26: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_27: [symbol; 12] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_1_28: [symbol; 2] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_1_29: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_1_30: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_1_31: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_1_32: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_1_33: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_1_34: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_1_35: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_1_36: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_37: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_38: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_39: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_40: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_41: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_42: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_43: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_44: [symbol; 8] = [
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_45: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_46: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_47: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_48: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_49: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_50: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_51: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_52: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_53: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_1_54: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
];
static mut s_1_55: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_1_56: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_1_57: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_1_58: [symbol; 6] = [
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_1_59: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_1_60: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_1_61: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_1_62: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_1_63: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_1_64: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_65: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_66: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_1_67: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_1_68: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_1_69: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_1_70: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 71] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_22 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_29 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_30 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_35 as *const symbol,
            substring_i: 34 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_37 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_38 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_39 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_40 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_42 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_43 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_44 as *const symbol,
            substring_i: 43 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_45 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_46 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_47 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_48 as *const symbol,
            substring_i: 47 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_49 as *const symbol,
            substring_i: 47 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_50 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_51 as *const symbol,
            substring_i: 50 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_52 as *const symbol,
            substring_i: 50 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_53 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_55 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_56 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_57 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_58 as *const symbol,
            substring_i: 57 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_59 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_61 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_62 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_63 as *const symbol,
            substring_i: 62 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_64 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_65 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_66 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_67 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_68 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_69 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_70 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_2_1: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_2_2: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_2_3: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_4: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_5: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_2_6: [symbol; 2] = [
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_7: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_8: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_9: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_10: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_11: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_12: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_13: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_14: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_15: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_16: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_17: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_18: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_2_19: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_2_20: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_2_21: [symbol; 4] = [
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_2_22: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_2_23: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_2_24: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_2_25: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_2_26: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_2_27: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_2_28: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_2_29: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_2_30: [symbol; 14] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_2_31: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_2_32: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_2_33: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_2_34: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_2_35: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_2_36: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_2_37: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_2_38: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_2_39: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 40] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_11 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_12 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_13 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_14 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_2_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_16 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_17 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_18 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_27 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_2_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_32 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_33 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_34 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_1: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_2: [symbol; 2] = [
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_3: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_4: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_5: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_6: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_7: [symbol; 6] = [
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_8: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_9: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_10: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_11: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_12: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_3_13: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_3_14: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_3_15: [symbol; 2] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_3_16: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_3_17: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_3_18: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_3_19: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_3_20: [symbol; 14] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_3_21: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_3_22: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_3_23: [symbol; 2] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_3_24: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_3_25: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_3_26: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_3_27: [symbol; 14] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_3_28: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_3_29: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_3_30: [symbol; 2] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_3_31: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_3_32: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_3_33: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_3_34: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_3_35: [symbol; 12] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_3_36: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_3_37: [symbol; 2] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_38: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_39: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_40: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_41: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_42: [symbol; 12] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_43: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_44: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_45: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_46: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_47: [symbol; 14] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_3_48: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_3_49: [symbol; 14] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_3_50: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_3_51: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_3_52: [symbol; 4] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_53: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_54: [symbol; 10] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xd6 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_55: [symbol; 8] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_56: [symbol; 6] = [
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut a_3: [among; 57] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_7 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_8 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_9 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_10 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_11 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_18 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_19 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_3_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_21 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_22 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_24 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_25 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_26 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_3_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_28 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_29 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_31 as *const symbol,
            substring_i: 30 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_32 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_33 as *const symbol,
            substring_i: 30 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_35 as *const symbol,
            substring_i: 34 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_3_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_38 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_39 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_40 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_41 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_42 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_43 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_44 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_45 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_46 as *const symbol,
            substring_i: 45 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_3_47 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_3_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_50 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_51 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_53 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_54 as *const symbol,
            substring_i: 53 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_55 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_56 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 5] = [
    209 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    4 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    128 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    18 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = out_grouping_U(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        1377 as ::core::ffi::c_int,
        1413 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret < 0 as ::core::ffi::c_int) {
        (*z).c += ret;
        *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
        let mut ret_0: ::core::ffi::c_int = in_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            1377 as ::core::ffi::c_int,
            1413 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_0 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_0;
            let mut ret_1: ::core::ffi::c_int = out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                1377 as ::core::ffi::c_int,
                1413 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_1 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_1;
                let mut ret_2: ::core::ffi::c_int = in_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    1377 as ::core::ffi::c_int,
                    1413 as ::core::ffi::c_int,
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
unsafe fn r_adjective(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_0 as *const among, 23 as ::core::ffi::c_int) == 0 {
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
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_1 as *const among, 71 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_noun(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_2 as *const among, 40 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_ending(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_3 as *const among, 57 as ::core::ffi::c_int) == 0 {
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
pub unsafe fn armenian_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_mark_regions(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_ending(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_verb(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m3;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_adjective(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m4;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_noun(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = (*z).l - m5;
    (*z).lb = mlimit1;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn armenian_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 2 as ::core::ffi::c_int);
}
pub unsafe fn armenian_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
