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
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_0_1: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_0_2: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_0_3: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_0_4: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_0_5: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_0_6: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_0_7: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_0_8: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
];
static mut s_0_9: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
];
static mut s_0_10: [symbol; 18] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_0_11: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_0_12: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_0_13: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_0_14: [symbol; 18] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_0_15: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_0_16: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 17] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_0_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_0_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_1_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_1_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 3] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
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
    ]
};
static mut s_2_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_2_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 3] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
static mut s_3_0: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_1: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_2: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_3: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_4: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x96 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_5: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_6: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_7: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_8: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_9: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_10: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_11: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_12: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_13: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_3_14: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_3_15: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_3_16: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_3_17: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_3_18: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_3_19: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_3_20: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_3_21: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_3_22: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_3_23: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_24: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_25: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_26: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_27: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_28: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_29: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_30: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_31: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_32: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_33: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_34: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_35: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_36: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_37: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_3_38: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_39: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_40: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_41: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_42: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_43: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_44: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_45: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_46: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_47: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_48: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_49: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_50: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_51: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_52: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_53: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_54: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_55: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_56: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_57: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_58: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_59: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_60: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_61: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_62: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_63: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_64: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_65: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_66: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_67: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_68: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_69: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_70: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_3_71: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_72: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_73: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_74: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_75: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_76: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_77: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_78: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_79: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_80: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_3_81: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_82: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_83: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_84: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_85: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_86: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_87: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_88: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_89: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x96 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_3_90: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_3: [among; 91] = unsafe {
    [
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_10 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_21 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_22 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
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
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_24 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_27 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_29 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_30 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_32 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_33 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_34 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_35 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_36 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_37 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_39 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_40 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_41 as *const symbol,
            substring_i: 40 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_42 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_43 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_45 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_46 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_47 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_49 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_50 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_3_51 as *const symbol,
            substring_i: 50 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_52 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_53 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_55 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_56 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_57 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_58 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_3_59 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_61 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_62 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_63 as *const symbol,
            substring_i: 62 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_64 as *const symbol,
            substring_i: 62 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_3_65 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_66 as *const symbol,
            substring_i: 62 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_67 as *const symbol,
            substring_i: 62 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_68 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_69 as *const symbol,
            substring_i: 68 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_70 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_71 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_72 as *const symbol,
            substring_i: 71 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_73 as *const symbol,
            substring_i: 71 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_74 as *const symbol,
            substring_i: 73 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_3_75 as *const symbol,
            substring_i: 74 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_3_76 as *const symbol,
            substring_i: 71 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_77 as *const symbol,
            substring_i: 71 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_78 as *const symbol,
            substring_i: 71 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_79 as *const symbol,
            substring_i: 71 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_80 as *const symbol,
            substring_i: 71 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_81 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_82 as *const symbol,
            substring_i: 81 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_83 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_84 as *const symbol,
            substring_i: 83 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_85 as *const symbol,
            substring_i: 83 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_86 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_87 as *const symbol,
            substring_i: 86 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_3_88 as *const symbol,
            substring_i: 86 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_89 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_90 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_2: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_4: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_5: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_6: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
unsafe fn r_remove_category_1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_0 as *const among,
        17 as ::core::ffi::c_int,
    );
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
        }
        2 => {
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_0 as *const symbol) == 0 {
                (*z).c = (*z).l - m2;
                if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_1 as *const symbol)
                    == 0
                {
                    (*z).c = (*z).l - m1;
                    let mut ret_0: ::core::ffi::c_int = slice_del(z);
                    if ret_0 < 0 as ::core::ffi::c_int {
                        return ret_0;
                    }
                }
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_check_category_2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 4 as ::core::ffi::c_int
        || 262 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_1 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_category_2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 4 as ::core::ffi::c_int
        || 262 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
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
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            if eq_s_b(z, 6 as ::core::ffi::c_int, &raw const s_2 as *const symbol) == 0 {
                (*z).c = (*z).l - m1;
                if eq_s_b(z, 6 as ::core::ffi::c_int, &raw const s_3 as *const symbol)
                    == 0
                {
                    (*z).c = (*z).l - m1;
                    if eq_s_b(
                        z,
                        6 as ::core::ffi::c_int,
                        &raw const s_4 as *const symbol,
                    ) == 0
                    {
                        (*z).c = (*z).l - m1;
                        if eq_s_b(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_5 as *const symbol,
                        ) == 0
                        {
                            return 0 as ::core::ffi::c_int;
                        }
                    }
                }
            }
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            if eq_s_b(z, 9 as ::core::ffi::c_int, &raw const s_6 as *const symbol) == 0 {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_category_3(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_3 as *const among, 91 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn nepali_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret: ::core::ffi::c_int = r_remove_category_1(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = (*z).l - m1;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    loop {
        let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_0: ::core::ffi::c_int = r_check_category_2(z);
        if !(ret_0 == 0 as ::core::ffi::c_int) {
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
            (*z).c = (*z).l - m5;
            let mut ret_1: ::core::ffi::c_int = r_remove_category_2(z);
            if !(ret_1 == 0 as ::core::ffi::c_int) {
                if ret_1 < 0 as ::core::ffi::c_int {
                    return ret_1;
                }
            }
        }
        (*z).c = (*z).l - m4;
        let mut ret_2: ::core::ffi::c_int = r_remove_category_3(z);
        if ret_2 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m3;
            break;
        } else if ret_2 < 0 as ::core::ffi::c_int {
            return ret_2
        }
    }
    (*z).c = (*z).l - m2;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn nepali_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 0 as ::core::ffi::c_int);
}
pub unsafe fn nepali_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
