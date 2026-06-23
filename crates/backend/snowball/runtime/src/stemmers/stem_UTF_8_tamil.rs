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
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_0_1: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_0_2: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_0_3: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 4] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_1_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
];
static mut s_1_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_1_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9e as ::core::ffi::c_int as symbol,
];
static mut s_1_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_1_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_1_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_1_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
];
static mut s_1_8: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_1_9: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 10] = unsafe {
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
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
    ]
};
static mut s_2_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_2_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_2_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 3] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
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
    ]
};
static mut s_3_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_3_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_3_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_3_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_3_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_3_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_3_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_3_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_3: [among; 8] = unsafe {
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
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_4_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_4_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 3] = unsafe {
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
            s: &raw const s_4_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_5_1: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_2: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_3: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_4: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_5: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_6: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_7: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_8: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_9: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_10: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_11: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_12: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_13: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_5_14: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_5_15: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_5_16: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_5: [among; 17] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_5_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_5_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_5_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_5_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_5_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_5_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_5_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_5_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_5_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_6_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_6_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_6_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_6_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_6_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut a_6: [among; 6] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_6_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_7_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_7_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_7_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_7_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_7_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut a_7: [among; 6] = unsafe {
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
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_7_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_8_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9e as ::core::ffi::c_int as symbol,
];
static mut s_8_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_8_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_8_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_8_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
];
static mut s_8_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_8_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_8_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_8_8: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_8_9: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_8_10: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_8: [among; 11] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
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
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_8_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_9_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_9_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_9_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_9_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_9_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_9_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_9_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_9_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_9_8: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_9: [among; 9] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_9_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_10_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_10_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_10_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut a_10: [among; 3] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_10_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_11_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_11_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
];
static mut s_11_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_11_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9e as ::core::ffi::c_int as symbol,
];
static mut s_11_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_11_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_11_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_11_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
];
static mut s_11_8: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_11_9: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_11: [among; 10] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_11_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_12_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_12_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_12_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_12_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_12_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_12_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut a_12: [among; 6] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_12_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_12_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_12_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_12_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_12_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_12_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_13_0: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_13_1: [symbol; 18] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_13_2: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_13_3: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut a_13: [among; 4] = unsafe {
    [
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_13_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_13_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_13_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_13_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_14_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_14_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_14_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut a_14: [among; 3] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_14_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_14_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_14_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_15_0: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_15_1: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_15: [among; 2] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_15_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_15_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_16_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_16_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_16_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_16_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_16_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_16_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_16_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_16_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_16: [among; 8] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_16_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_16_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_16_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_16_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_16_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_16_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_16_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_16_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_17_0: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_1: [symbol; 18] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_2: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_3: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_4: [symbol; 18] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_5: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_6: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_17_7: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_17_8: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_17_9: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_17_10: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_17_11: [symbol; 21] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_17_12: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_17_13: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_17_14: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_17_15: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_17_16: [symbol; 18] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_17_17: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_17_18: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_17_19: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_17_20: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_17_21: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_17_22: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_17_23: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_17_24: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_17_25: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_17: [among; 26] = unsafe {
    [
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_17_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_17_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_17_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_17_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_17_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_17_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_17_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_17_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 21 as ::core::ffi::c_int,
            s: &raw const s_17_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_17_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_17_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_17_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_17_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_17_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_17_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_17_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_17_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_17_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_18_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_18_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_18_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_18_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_18_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_18_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_18_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_18_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_18: [among; 8] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_18_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_18_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_18_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_18_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_18_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_18_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_18_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_18_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_19_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_19_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_19_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_19_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_19_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_19_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_19_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_19_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_19: [among; 8] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_19_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_19_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_19_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_19_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_19_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_19_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_19_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_19_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_20_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_20_1: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_20_2: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_20_3: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_20_4: [symbol; 21] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_20_5: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_20_6: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_20_7: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_20_8: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_9: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_10: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_11: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_12: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_13: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_14: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_15: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_16: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_17: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_18: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_19: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_20: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20_21: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut a_20: [among; 22] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_20_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 21 as ::core::ffi::c_int,
            s: &raw const s_20_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_20_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_20_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_20_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_20_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_20_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_20_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_20_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_20_16 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_17 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_18 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_20_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_20_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_21_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_21_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_21_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_21_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_21_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_21_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut a_21: [among; 6] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_21_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_21_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_21_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_21_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_21_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_21_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_22_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_22_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_22_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_22_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_22_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_22_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut a_22: [among; 6] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_22_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_22_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_22_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_22_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_22_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_22_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_23_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_23_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_23_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_23_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_23_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_23_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_23_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_23_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_23_8: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x90 as ::core::ffi::c_int as symbol,
];
static mut s_23_9: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x92 as ::core::ffi::c_int as symbol,
];
static mut s_23_10: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x93 as ::core::ffi::c_int as symbol,
];
static mut s_23_11: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x94 as ::core::ffi::c_int as symbol,
];
static mut a_23: [among; 12] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_23_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_24_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_24_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_24_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_24_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_24_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_24_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_24_6: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_24_7: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_24: [among; 8] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_24_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_24_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_24_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_24_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_24_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_24_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_24_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_24_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_25_0: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_25_1: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_25_2: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_25_3: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_25_4: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_25_5: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_25_6: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_7: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_8: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_9: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_10: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_11: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_12: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_13: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_14: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_15: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_16: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_17: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_18: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_19: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_20: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_21: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_22: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_23: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_24: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_25: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_26: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_27: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_28: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_29: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_30: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_31: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_32: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_33: [symbol; 24] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_34: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_35: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_36: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_37: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_38: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_39: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25_40: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_25_41: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_25_42: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_25_43: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_25_44: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_25_45: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut a_25: [among; 46] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_25_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_25_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_25_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_25_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_25_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_12 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_31 as *const symbol,
            substring_i: 30 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_32 as *const symbol,
            substring_i: 30 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 24 as ::core::ffi::c_int,
            s: &raw const s_25_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_25_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_25_39 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_25_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_25_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_25_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_25_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_25_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_25_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_26_0: [symbol; 18] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_26_1: [symbol; 21] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_26_2: [symbol; 12] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_26_3: [symbol; 15] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_26_4: [symbol; 18] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_26_5: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut a_26: [among; 6] = unsafe {
    [
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_26_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 21 as ::core::ffi::c_int,
            s: &raw const s_26_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_26_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 15 as ::core::ffi::c_int,
            s: &raw const s_26_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_26_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_26_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_0: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x93 as ::core::ffi::c_int as symbol,
];
static mut s_1: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x92 as ::core::ffi::c_int as symbol,
];
static mut s_2: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_3: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_4: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_5: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_6: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_7: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_8: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_9: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_10: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_11: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_12: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_13: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_14: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_15: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_16: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_17: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_18: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_19: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_20: [symbol; 6] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_21: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_22: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_23: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_24: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_25: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_26: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_27: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
];
static mut s_28: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_29: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_30: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_31: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_32: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_33: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_34: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_35: [symbol; 9] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_36: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_37: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_38: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_39: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_40: [symbol; 3] = [
    0xe0 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
unsafe fn r_has_min_length(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (len_utf8((*z).p) > 4 as ::core::ffi::c_int) as ::core::ffi::c_int;
}
unsafe fn r_fix_va_start(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    if (*z).c + 5 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 5 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 4 as ::core::ffi::c_int
        || 3078 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c + 5 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among(z, &raw const a_0 as *const among, 4 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_0 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_1 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_2 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_3 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_fix_endings(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    loop {
        let mut c2: ::core::ffi::c_int = (*z).c;
        let mut ret: ::core::ffi::c_int = r_fix_ending(z);
        if ret == 0 as ::core::ffi::c_int {
            (*z).c = c2;
            break;
        } else if ret < 0 as ::core::ffi::c_int {
            return ret
        }
    }
    (*z).c = c1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_question_prefixes(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    (*z).bra = (*z).c;
    if eq_s(z, 3 as ::core::ffi::c_int, &raw const s_4 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if find_among(z, &raw const a_1 as *const among, 10 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if eq_s(z, 3 as ::core::ffi::c_int, &raw const s_5 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_fix_va_start(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = c1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_fix_ending(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_5 as *const among,
        17 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        current_block = 2092442392310703162;
    } else {
        (*z).bra = (*z).c;
        match among_var {
            1 => {
                current_block = 16836374352058805143;
                match current_block {
                    13423106481271102964 => {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_11 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 5666559563733453363;
                    }
                    1262584022574969615 => {
                        if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                            current_block = 2092442392310703162;
                        } else {
                            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_9 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m3;
                                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_10 as *const symbol,
                                );
                                if ret_4 < 0 as ::core::ffi::c_int {
                                    return ret_4;
                                }
                                current_block = 5666559563733453363;
                            } else {
                                current_block = 2092442392310703162;
                            }
                        }
                    }
                    13536709405535804910 => {
                        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_2 as *const among,
                            3 as ::core::ffi::c_int,
                        ) == 0
                        {
                            current_block = 2092442392310703162;
                        } else {
                            (*z).c = (*z).l - m_test2;
                            let mut ret_0: ::core::ffi::c_int = slice_del(z);
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                            current_block = 5666559563733453363;
                        }
                    }
                    15623170874954075441 => {
                        if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
                                && *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 141 as ::core::ffi::c_int
                        {
                            among_var = 2 as ::core::ffi::c_int;
                        } else {
                            among_var = find_among_b(
                                z,
                                &raw const a_4 as *const among,
                                3 as ::core::ffi::c_int,
                            );
                        }
                        match among_var {
                            1 => {
                                let mut ret_7: ::core::ffi::c_int = slice_del(z);
                                if ret_7 < 0 as ::core::ffi::c_int {
                                    return ret_7;
                                }
                            }
                            2 => {
                                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_12 as *const symbol,
                                );
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                            }
                            _ => {}
                        }
                        current_block = 5666559563733453363;
                    }
                    16836374352058805143 => {
                        let mut ret: ::core::ffi::c_int = slice_del(z);
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 5666559563733453363;
                    }
                    12137889358298489534 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 5666559563733453363;
                    }
                    193480355586644163 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_7 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 5666559563733453363;
                    }
                    7955586766290726337 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_8 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 5666559563733453363;
                    }
                    _ => {
                        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_3 as *const among,
                            8 as ::core::ffi::c_int,
                        ) == 0
                        {
                            (*z).c = (*z).l - m4;
                            let mut ret_6: ::core::ffi::c_int = slice_del(z);
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 5666559563733453363;
                        } else {
                            current_block = 2092442392310703162;
                        }
                    }
                }
            }
            2 => {
                current_block = 13536709405535804910;
                match current_block {
                    13423106481271102964 => {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_11 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 5666559563733453363;
                    }
                    1262584022574969615 => {
                        if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                            current_block = 2092442392310703162;
                        } else {
                            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_9 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m3;
                                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_10 as *const symbol,
                                );
                                if ret_4 < 0 as ::core::ffi::c_int {
                                    return ret_4;
                                }
                                current_block = 5666559563733453363;
                            } else {
                                current_block = 2092442392310703162;
                            }
                        }
                    }
                    13536709405535804910 => {
                        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_2 as *const among,
                            3 as ::core::ffi::c_int,
                        ) == 0
                        {
                            current_block = 2092442392310703162;
                        } else {
                            (*z).c = (*z).l - m_test2;
                            let mut ret_0: ::core::ffi::c_int = slice_del(z);
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                            current_block = 5666559563733453363;
                        }
                    }
                    15623170874954075441 => {
                        if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
                                && *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 141 as ::core::ffi::c_int
                        {
                            among_var = 2 as ::core::ffi::c_int;
                        } else {
                            among_var = find_among_b(
                                z,
                                &raw const a_4 as *const among,
                                3 as ::core::ffi::c_int,
                            );
                        }
                        match among_var {
                            1 => {
                                let mut ret_7: ::core::ffi::c_int = slice_del(z);
                                if ret_7 < 0 as ::core::ffi::c_int {
                                    return ret_7;
                                }
                            }
                            2 => {
                                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_12 as *const symbol,
                                );
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                            }
                            _ => {}
                        }
                        current_block = 5666559563733453363;
                    }
                    16836374352058805143 => {
                        let mut ret: ::core::ffi::c_int = slice_del(z);
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 5666559563733453363;
                    }
                    12137889358298489534 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 5666559563733453363;
                    }
                    193480355586644163 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_7 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 5666559563733453363;
                    }
                    7955586766290726337 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_8 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 5666559563733453363;
                    }
                    _ => {
                        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_3 as *const among,
                            8 as ::core::ffi::c_int,
                        ) == 0
                        {
                            (*z).c = (*z).l - m4;
                            let mut ret_6: ::core::ffi::c_int = slice_del(z);
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 5666559563733453363;
                        } else {
                            current_block = 2092442392310703162;
                        }
                    }
                }
            }
            3 => {
                current_block = 12137889358298489534;
                match current_block {
                    13423106481271102964 => {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_11 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 5666559563733453363;
                    }
                    1262584022574969615 => {
                        if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                            current_block = 2092442392310703162;
                        } else {
                            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_9 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m3;
                                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_10 as *const symbol,
                                );
                                if ret_4 < 0 as ::core::ffi::c_int {
                                    return ret_4;
                                }
                                current_block = 5666559563733453363;
                            } else {
                                current_block = 2092442392310703162;
                            }
                        }
                    }
                    13536709405535804910 => {
                        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_2 as *const among,
                            3 as ::core::ffi::c_int,
                        ) == 0
                        {
                            current_block = 2092442392310703162;
                        } else {
                            (*z).c = (*z).l - m_test2;
                            let mut ret_0: ::core::ffi::c_int = slice_del(z);
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                            current_block = 5666559563733453363;
                        }
                    }
                    15623170874954075441 => {
                        if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
                                && *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 141 as ::core::ffi::c_int
                        {
                            among_var = 2 as ::core::ffi::c_int;
                        } else {
                            among_var = find_among_b(
                                z,
                                &raw const a_4 as *const among,
                                3 as ::core::ffi::c_int,
                            );
                        }
                        match among_var {
                            1 => {
                                let mut ret_7: ::core::ffi::c_int = slice_del(z);
                                if ret_7 < 0 as ::core::ffi::c_int {
                                    return ret_7;
                                }
                            }
                            2 => {
                                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_12 as *const symbol,
                                );
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                            }
                            _ => {}
                        }
                        current_block = 5666559563733453363;
                    }
                    16836374352058805143 => {
                        let mut ret: ::core::ffi::c_int = slice_del(z);
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 5666559563733453363;
                    }
                    12137889358298489534 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 5666559563733453363;
                    }
                    193480355586644163 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_7 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 5666559563733453363;
                    }
                    7955586766290726337 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_8 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 5666559563733453363;
                    }
                    _ => {
                        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_3 as *const among,
                            8 as ::core::ffi::c_int,
                        ) == 0
                        {
                            (*z).c = (*z).l - m4;
                            let mut ret_6: ::core::ffi::c_int = slice_del(z);
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 5666559563733453363;
                        } else {
                            current_block = 2092442392310703162;
                        }
                    }
                }
            }
            4 => {
                current_block = 193480355586644163;
                match current_block {
                    13423106481271102964 => {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_11 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 5666559563733453363;
                    }
                    1262584022574969615 => {
                        if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                            current_block = 2092442392310703162;
                        } else {
                            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_9 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m3;
                                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_10 as *const symbol,
                                );
                                if ret_4 < 0 as ::core::ffi::c_int {
                                    return ret_4;
                                }
                                current_block = 5666559563733453363;
                            } else {
                                current_block = 2092442392310703162;
                            }
                        }
                    }
                    13536709405535804910 => {
                        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_2 as *const among,
                            3 as ::core::ffi::c_int,
                        ) == 0
                        {
                            current_block = 2092442392310703162;
                        } else {
                            (*z).c = (*z).l - m_test2;
                            let mut ret_0: ::core::ffi::c_int = slice_del(z);
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                            current_block = 5666559563733453363;
                        }
                    }
                    15623170874954075441 => {
                        if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
                                && *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 141 as ::core::ffi::c_int
                        {
                            among_var = 2 as ::core::ffi::c_int;
                        } else {
                            among_var = find_among_b(
                                z,
                                &raw const a_4 as *const among,
                                3 as ::core::ffi::c_int,
                            );
                        }
                        match among_var {
                            1 => {
                                let mut ret_7: ::core::ffi::c_int = slice_del(z);
                                if ret_7 < 0 as ::core::ffi::c_int {
                                    return ret_7;
                                }
                            }
                            2 => {
                                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_12 as *const symbol,
                                );
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                            }
                            _ => {}
                        }
                        current_block = 5666559563733453363;
                    }
                    16836374352058805143 => {
                        let mut ret: ::core::ffi::c_int = slice_del(z);
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 5666559563733453363;
                    }
                    12137889358298489534 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 5666559563733453363;
                    }
                    193480355586644163 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_7 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 5666559563733453363;
                    }
                    7955586766290726337 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_8 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 5666559563733453363;
                    }
                    _ => {
                        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_3 as *const among,
                            8 as ::core::ffi::c_int,
                        ) == 0
                        {
                            (*z).c = (*z).l - m4;
                            let mut ret_6: ::core::ffi::c_int = slice_del(z);
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 5666559563733453363;
                        } else {
                            current_block = 2092442392310703162;
                        }
                    }
                }
            }
            5 => {
                current_block = 7955586766290726337;
                match current_block {
                    13423106481271102964 => {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_11 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 5666559563733453363;
                    }
                    1262584022574969615 => {
                        if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                            current_block = 2092442392310703162;
                        } else {
                            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_9 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m3;
                                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_10 as *const symbol,
                                );
                                if ret_4 < 0 as ::core::ffi::c_int {
                                    return ret_4;
                                }
                                current_block = 5666559563733453363;
                            } else {
                                current_block = 2092442392310703162;
                            }
                        }
                    }
                    13536709405535804910 => {
                        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_2 as *const among,
                            3 as ::core::ffi::c_int,
                        ) == 0
                        {
                            current_block = 2092442392310703162;
                        } else {
                            (*z).c = (*z).l - m_test2;
                            let mut ret_0: ::core::ffi::c_int = slice_del(z);
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                            current_block = 5666559563733453363;
                        }
                    }
                    15623170874954075441 => {
                        if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
                                && *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 141 as ::core::ffi::c_int
                        {
                            among_var = 2 as ::core::ffi::c_int;
                        } else {
                            among_var = find_among_b(
                                z,
                                &raw const a_4 as *const among,
                                3 as ::core::ffi::c_int,
                            );
                        }
                        match among_var {
                            1 => {
                                let mut ret_7: ::core::ffi::c_int = slice_del(z);
                                if ret_7 < 0 as ::core::ffi::c_int {
                                    return ret_7;
                                }
                            }
                            2 => {
                                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_12 as *const symbol,
                                );
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                            }
                            _ => {}
                        }
                        current_block = 5666559563733453363;
                    }
                    16836374352058805143 => {
                        let mut ret: ::core::ffi::c_int = slice_del(z);
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 5666559563733453363;
                    }
                    12137889358298489534 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 5666559563733453363;
                    }
                    193480355586644163 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_7 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 5666559563733453363;
                    }
                    7955586766290726337 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_8 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 5666559563733453363;
                    }
                    _ => {
                        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_3 as *const among,
                            8 as ::core::ffi::c_int,
                        ) == 0
                        {
                            (*z).c = (*z).l - m4;
                            let mut ret_6: ::core::ffi::c_int = slice_del(z);
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 5666559563733453363;
                        } else {
                            current_block = 2092442392310703162;
                        }
                    }
                }
            }
            6 => {
                current_block = 1262584022574969615;
                match current_block {
                    13423106481271102964 => {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_11 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 5666559563733453363;
                    }
                    1262584022574969615 => {
                        if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                            current_block = 2092442392310703162;
                        } else {
                            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_9 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m3;
                                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_10 as *const symbol,
                                );
                                if ret_4 < 0 as ::core::ffi::c_int {
                                    return ret_4;
                                }
                                current_block = 5666559563733453363;
                            } else {
                                current_block = 2092442392310703162;
                            }
                        }
                    }
                    13536709405535804910 => {
                        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_2 as *const among,
                            3 as ::core::ffi::c_int,
                        ) == 0
                        {
                            current_block = 2092442392310703162;
                        } else {
                            (*z).c = (*z).l - m_test2;
                            let mut ret_0: ::core::ffi::c_int = slice_del(z);
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                            current_block = 5666559563733453363;
                        }
                    }
                    15623170874954075441 => {
                        if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
                                && *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 141 as ::core::ffi::c_int
                        {
                            among_var = 2 as ::core::ffi::c_int;
                        } else {
                            among_var = find_among_b(
                                z,
                                &raw const a_4 as *const among,
                                3 as ::core::ffi::c_int,
                            );
                        }
                        match among_var {
                            1 => {
                                let mut ret_7: ::core::ffi::c_int = slice_del(z);
                                if ret_7 < 0 as ::core::ffi::c_int {
                                    return ret_7;
                                }
                            }
                            2 => {
                                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_12 as *const symbol,
                                );
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                            }
                            _ => {}
                        }
                        current_block = 5666559563733453363;
                    }
                    16836374352058805143 => {
                        let mut ret: ::core::ffi::c_int = slice_del(z);
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 5666559563733453363;
                    }
                    12137889358298489534 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 5666559563733453363;
                    }
                    193480355586644163 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_7 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 5666559563733453363;
                    }
                    7955586766290726337 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_8 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 5666559563733453363;
                    }
                    _ => {
                        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_3 as *const among,
                            8 as ::core::ffi::c_int,
                        ) == 0
                        {
                            (*z).c = (*z).l - m4;
                            let mut ret_6: ::core::ffi::c_int = slice_del(z);
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 5666559563733453363;
                        } else {
                            current_block = 2092442392310703162;
                        }
                    }
                }
            }
            7 => {
                current_block = 13423106481271102964;
                match current_block {
                    13423106481271102964 => {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_11 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 5666559563733453363;
                    }
                    1262584022574969615 => {
                        if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                            current_block = 2092442392310703162;
                        } else {
                            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_9 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m3;
                                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_10 as *const symbol,
                                );
                                if ret_4 < 0 as ::core::ffi::c_int {
                                    return ret_4;
                                }
                                current_block = 5666559563733453363;
                            } else {
                                current_block = 2092442392310703162;
                            }
                        }
                    }
                    13536709405535804910 => {
                        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_2 as *const among,
                            3 as ::core::ffi::c_int,
                        ) == 0
                        {
                            current_block = 2092442392310703162;
                        } else {
                            (*z).c = (*z).l - m_test2;
                            let mut ret_0: ::core::ffi::c_int = slice_del(z);
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                            current_block = 5666559563733453363;
                        }
                    }
                    15623170874954075441 => {
                        if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
                                && *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 141 as ::core::ffi::c_int
                        {
                            among_var = 2 as ::core::ffi::c_int;
                        } else {
                            among_var = find_among_b(
                                z,
                                &raw const a_4 as *const among,
                                3 as ::core::ffi::c_int,
                            );
                        }
                        match among_var {
                            1 => {
                                let mut ret_7: ::core::ffi::c_int = slice_del(z);
                                if ret_7 < 0 as ::core::ffi::c_int {
                                    return ret_7;
                                }
                            }
                            2 => {
                                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_12 as *const symbol,
                                );
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                            }
                            _ => {}
                        }
                        current_block = 5666559563733453363;
                    }
                    16836374352058805143 => {
                        let mut ret: ::core::ffi::c_int = slice_del(z);
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 5666559563733453363;
                    }
                    12137889358298489534 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 5666559563733453363;
                    }
                    193480355586644163 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_7 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 5666559563733453363;
                    }
                    7955586766290726337 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_8 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 5666559563733453363;
                    }
                    _ => {
                        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_3 as *const among,
                            8 as ::core::ffi::c_int,
                        ) == 0
                        {
                            (*z).c = (*z).l - m4;
                            let mut ret_6: ::core::ffi::c_int = slice_del(z);
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 5666559563733453363;
                        } else {
                            current_block = 2092442392310703162;
                        }
                    }
                }
            }
            8 => {
                current_block = 14072441030219150333;
                match current_block {
                    13423106481271102964 => {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_11 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 5666559563733453363;
                    }
                    1262584022574969615 => {
                        if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                            current_block = 2092442392310703162;
                        } else {
                            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_9 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m3;
                                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_10 as *const symbol,
                                );
                                if ret_4 < 0 as ::core::ffi::c_int {
                                    return ret_4;
                                }
                                current_block = 5666559563733453363;
                            } else {
                                current_block = 2092442392310703162;
                            }
                        }
                    }
                    13536709405535804910 => {
                        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_2 as *const among,
                            3 as ::core::ffi::c_int,
                        ) == 0
                        {
                            current_block = 2092442392310703162;
                        } else {
                            (*z).c = (*z).l - m_test2;
                            let mut ret_0: ::core::ffi::c_int = slice_del(z);
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                            current_block = 5666559563733453363;
                        }
                    }
                    15623170874954075441 => {
                        if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
                                && *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 141 as ::core::ffi::c_int
                        {
                            among_var = 2 as ::core::ffi::c_int;
                        } else {
                            among_var = find_among_b(
                                z,
                                &raw const a_4 as *const among,
                                3 as ::core::ffi::c_int,
                            );
                        }
                        match among_var {
                            1 => {
                                let mut ret_7: ::core::ffi::c_int = slice_del(z);
                                if ret_7 < 0 as ::core::ffi::c_int {
                                    return ret_7;
                                }
                            }
                            2 => {
                                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_12 as *const symbol,
                                );
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                            }
                            _ => {}
                        }
                        current_block = 5666559563733453363;
                    }
                    16836374352058805143 => {
                        let mut ret: ::core::ffi::c_int = slice_del(z);
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 5666559563733453363;
                    }
                    12137889358298489534 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 5666559563733453363;
                    }
                    193480355586644163 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_7 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 5666559563733453363;
                    }
                    7955586766290726337 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_8 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 5666559563733453363;
                    }
                    _ => {
                        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_3 as *const among,
                            8 as ::core::ffi::c_int,
                        ) == 0
                        {
                            (*z).c = (*z).l - m4;
                            let mut ret_6: ::core::ffi::c_int = slice_del(z);
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 5666559563733453363;
                        } else {
                            current_block = 2092442392310703162;
                        }
                    }
                }
            }
            9 => {
                current_block = 15623170874954075441;
                match current_block {
                    13423106481271102964 => {
                        let mut ret_5: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_11 as *const symbol,
                        );
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 5666559563733453363;
                    }
                    1262584022574969615 => {
                        if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                            current_block = 2092442392310703162;
                        } else {
                            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                3 as ::core::ffi::c_int,
                                &raw const s_9 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m3;
                                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_10 as *const symbol,
                                );
                                if ret_4 < 0 as ::core::ffi::c_int {
                                    return ret_4;
                                }
                                current_block = 5666559563733453363;
                            } else {
                                current_block = 2092442392310703162;
                            }
                        }
                    }
                    13536709405535804910 => {
                        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_2 as *const among,
                            3 as ::core::ffi::c_int,
                        ) == 0
                        {
                            current_block = 2092442392310703162;
                        } else {
                            (*z).c = (*z).l - m_test2;
                            let mut ret_0: ::core::ffi::c_int = slice_del(z);
                            if ret_0 < 0 as ::core::ffi::c_int {
                                return ret_0;
                            }
                            current_block = 5666559563733453363;
                        }
                    }
                    15623170874954075441 => {
                        if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
                                && *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 141 as ::core::ffi::c_int
                        {
                            among_var = 2 as ::core::ffi::c_int;
                        } else {
                            among_var = find_among_b(
                                z,
                                &raw const a_4 as *const among,
                                3 as ::core::ffi::c_int,
                            );
                        }
                        match among_var {
                            1 => {
                                let mut ret_7: ::core::ffi::c_int = slice_del(z);
                                if ret_7 < 0 as ::core::ffi::c_int {
                                    return ret_7;
                                }
                            }
                            2 => {
                                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                                    z,
                                    6 as ::core::ffi::c_int,
                                    &raw const s_12 as *const symbol,
                                );
                                if ret_8 < 0 as ::core::ffi::c_int {
                                    return ret_8;
                                }
                            }
                            _ => {}
                        }
                        current_block = 5666559563733453363;
                    }
                    16836374352058805143 => {
                        let mut ret: ::core::ffi::c_int = slice_del(z);
                        if ret < 0 as ::core::ffi::c_int {
                            return ret;
                        }
                        current_block = 5666559563733453363;
                    }
                    12137889358298489534 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_6 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 5666559563733453363;
                    }
                    193480355586644163 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_7 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 5666559563733453363;
                    }
                    7955586766290726337 => {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            6 as ::core::ffi::c_int,
                            &raw const s_8 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 5666559563733453363;
                    }
                    _ => {
                        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_3 as *const among,
                            8 as ::core::ffi::c_int,
                        ) == 0
                        {
                            (*z).c = (*z).l - m4;
                            let mut ret_6: ::core::ffi::c_int = slice_del(z);
                            if ret_6 < 0 as ::core::ffi::c_int {
                                return ret_6;
                            }
                            current_block = 5666559563733453363;
                        } else {
                            current_block = 2092442392310703162;
                        }
                    }
                }
            }
            _ => {
                current_block = 5666559563733453363;
            }
        }
    }
    match current_block {
        2092442392310703162 => {
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_13 as *const symbol) == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
            if find_among_b(z, &raw const a_6 as *const among, 6 as ::core::ffi::c_int)
                == 0
            {
                (*z).c = (*z).l - m5;
                if find_among_b(
                    z,
                    &raw const a_8 as *const among,
                    11 as ::core::ffi::c_int,
                ) == 0
                {
                    current_block = 1196376431729137931;
                } else {
                    (*z).bra = (*z).c;
                    if eq_s_b(
                        z,
                        3 as ::core::ffi::c_int,
                        &raw const s_15 as *const symbol,
                    ) == 0
                    {
                        current_block = 1196376431729137931;
                    } else {
                        let mut ret_10: ::core::ffi::c_int = slice_del(z);
                        if ret_10 < 0 as ::core::ffi::c_int {
                            return ret_10;
                        }
                        current_block = 5666559563733453363;
                    }
                }
                match current_block {
                    5666559563733453363 => {}
                    _ => {
                        (*z).c = (*z).l - m5;
                        let mut m_test7: ::core::ffi::c_int = (*z).l - (*z).c;
                        if find_among_b(
                            z,
                            &raw const a_9 as *const among,
                            9 as ::core::ffi::c_int,
                        ) == 0
                        {
                            return 0 as ::core::ffi::c_int;
                        }
                        (*z).c = (*z).l - m_test7;
                        (*z).bra = (*z).c;
                        let mut ret_11: ::core::ffi::c_int = slice_del(z);
                        if ret_11 < 0 as ::core::ffi::c_int {
                            return ret_11;
                        }
                    }
                }
            } else {
                let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_14 as *const symbol)
                    == 0
                {
                    (*z).c = (*z).l - m6;
                } else if find_among_b(
                    z,
                    &raw const a_7 as *const among,
                    6 as ::core::ffi::c_int,
                ) == 0
                {
                    (*z).c = (*z).l - m6;
                }
                (*z).bra = (*z).c;
                let mut ret_9: ::core::ffi::c_int = slice_del(z);
                if ret_9 < 0 as ::core::ffi::c_int {
                    return ret_9;
                }
            }
        }
        _ => {}
    }
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_pronoun_prefixes(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    (*z).bra = (*z).c;
    if (*z).c + 2 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 2 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 4 as ::core::ffi::c_int
        || 672 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c + 2 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among(z, &raw const a_10 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if find_among(z, &raw const a_11 as *const among, 10 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if eq_s(z, 3 as ::core::ffi::c_int, &raw const s_16 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_fix_va_start(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = c1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_plural_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    (*z).ket = (*z).c;
    if (*z).c - 8 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 141 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_13 as *const among,
        4 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            if find_among_b(z, &raw const a_12 as *const among, 6 as ::core::ffi::c_int)
                == 0
            {
                (*z).c = (*z).l - m1;
                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                    z,
                    3 as ::core::ffi::c_int,
                    &raw const s_18 as *const symbol,
                );
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
            } else {
                let mut ret: ::core::ffi::c_int = slice_from_s(
                    z,
                    9 as ::core::ffi::c_int,
                    &raw const s_17 as *const symbol,
                );
                if ret < 0 as ::core::ffi::c_int {
                    return ret;
                }
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_19 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        3 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_20 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        4 => {
            let mut ret_3: ::core::ffi::c_int = slice_del(z);
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        _ => {}
    }
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_question_suffixes(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_has_min_length(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !(find_among_b(z, &raw const a_14 as *const among, 3 as ::core::ffi::c_int) == 0)
    {
        (*z).bra = (*z).c;
        let mut ret_0: ::core::ffi::c_int = slice_from_s(
            z,
            3 as ::core::ffi::c_int,
            &raw const s_21 as *const symbol,
        );
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
    }
    (*z).c = (*z).l - m1;
    (*z).c = (*z).lb;
    let mut ret_1: ::core::ffi::c_int = r_fix_endings(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_command_suffixes(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_has_min_length(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    (*z).ket = (*z).c;
    if (*z).c - 5 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 191 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_15 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_um(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_has_min_length(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    (*z).ket = (*z).c;
    if eq_s_b(z, 9 as ::core::ffi::c_int, &raw const s_22 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        3 as ::core::ffi::c_int,
        &raw const s_23 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).lb;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_fix_ending(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = c1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_common_word_endings(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut ret: ::core::ffi::c_int = r_has_min_length(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_17 as *const among,
        26 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_24 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            if find_among_b(z, &raw const a_16 as *const among, 8 as ::core::ffi::c_int)
                == 0
            {
                (*z).c = (*z).l - m1;
            } else {
                return 0 as ::core::ffi::c_int
            }
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_25 as *const symbol,
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
    (*z).c = (*z).lb;
    let mut ret_3: ::core::ffi::c_int = r_fix_endings(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_vetrumai_urupukal(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut ret: ::core::ffi::c_int = r_has_min_length(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 4 as ::core::ffi::c_int
        || -(2147475197 as ::core::ffi::c_int)
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        current_block = 17784659355455874416;
    } else {
        among_var = find_among_b(
            z,
            &raw const a_20 as *const among,
            22 as ::core::ffi::c_int,
        );
        if among_var == 0 {
            current_block = 17784659355455874416;
        } else {
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    let mut ret_0: ::core::ffi::c_int = slice_del(z);
                    if ret_0 < 0 as ::core::ffi::c_int {
                        return ret_0;
                    }
                    current_block = 12497913735442871383;
                }
                2 => {
                    let mut ret_1: ::core::ffi::c_int = slice_from_s(
                        z,
                        3 as ::core::ffi::c_int,
                        &raw const s_26 as *const symbol,
                    );
                    if ret_1 < 0 as ::core::ffi::c_int {
                        return ret_1;
                    }
                    current_block = 12497913735442871383;
                }
                3 => {
                    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                    if eq_s_b(
                        z,
                        3 as ::core::ffi::c_int,
                        &raw const s_27 as *const symbol,
                    ) == 0
                    {
                        (*z).c = (*z).l - m3;
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_28 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                        current_block = 12497913735442871383;
                    } else {
                        current_block = 17784659355455874416;
                    }
                }
                4 => {
                    if len_utf8((*z).p) < 7 as ::core::ffi::c_int {
                        current_block = 17784659355455874416;
                    } else {
                        let mut ret_3: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_29 as *const symbol,
                        );
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        current_block = 12497913735442871383;
                    }
                }
                5 => {
                    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                    if find_among_b(
                        z,
                        &raw const a_18 as *const among,
                        8 as ::core::ffi::c_int,
                    ) == 0
                    {
                        (*z).c = (*z).l - m4;
                        let mut ret_4: ::core::ffi::c_int = slice_from_s(
                            z,
                            3 as ::core::ffi::c_int,
                            &raw const s_30 as *const symbol,
                        );
                        if ret_4 < 0 as ::core::ffi::c_int {
                            return ret_4;
                        }
                        current_block = 12497913735442871383;
                    } else {
                        current_block = 17784659355455874416;
                    }
                }
                6 => {
                    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
                    if find_among_b(
                        z,
                        &raw const a_19 as *const among,
                        8 as ::core::ffi::c_int,
                    ) == 0
                    {
                        (*z).c = (*z).l - m5;
                        let mut ret_5: ::core::ffi::c_int = slice_del(z);
                        if ret_5 < 0 as ::core::ffi::c_int {
                            return ret_5;
                        }
                        current_block = 12497913735442871383;
                    } else {
                        current_block = 17784659355455874416;
                    }
                }
                7 => {
                    let mut ret_6: ::core::ffi::c_int = slice_from_s(
                        z,
                        3 as ::core::ffi::c_int,
                        &raw const s_31 as *const symbol,
                    );
                    if ret_6 < 0 as ::core::ffi::c_int {
                        return ret_6;
                    }
                    current_block = 12497913735442871383;
                }
                _ => {
                    current_block = 12497913735442871383;
                }
            }
            match current_block {
                17784659355455874416 => {}
                _ => {
                    (*z).c = (*z).l - m_test2;
                    current_block = 4911118195659552435;
                }
            }
        }
    }
    match current_block {
        17784659355455874416 => {
            (*z).c = (*z).l - m1;
            let mut m_test6: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_32 as *const symbol) == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
            if find_among_b(z, &raw const a_21 as *const among, 6 as ::core::ffi::c_int)
                == 0
            {
                (*z).c = (*z).l - m8;
            } else {
                (*z).c = (*z).l - m7;
                let mut m_test9: ::core::ffi::c_int = (*z).l - (*z).c;
                if find_among_b(
                    z,
                    &raw const a_22 as *const among,
                    6 as ::core::ffi::c_int,
                ) == 0
                {
                    return 0 as ::core::ffi::c_int;
                }
                if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_33 as *const symbol)
                    == 0
                {
                    return 0 as ::core::ffi::c_int;
                }
                (*z).c = (*z).l - m_test9;
            }
            (*z).bra = (*z).c;
            let mut ret_7: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_34 as *const symbol,
            );
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
            (*z).c = (*z).l - m_test6;
        }
        _ => {}
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !(eq_s_b(z, 9 as ::core::ffi::c_int, &raw const s_35 as *const symbol) == 0) {
        (*z).bra = (*z).c;
        let mut ret_8: ::core::ffi::c_int = slice_from_s(
            z,
            3 as ::core::ffi::c_int,
            &raw const s_36 as *const symbol,
        );
        if ret_8 < 0 as ::core::ffi::c_int {
            return ret_8;
        }
    }
    (*z).c = (*z).l - m10;
    (*z).c = (*z).lb;
    let mut ret_9: ::core::ffi::c_int = r_fix_endings(z);
    if ret_9 < 0 as ::core::ffi::c_int {
        return ret_9;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_tense_suffixes(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    loop {
        let mut c1: ::core::ffi::c_int = (*z).c;
        if *(*z).I.offset(1 as ::core::ffi::c_int as isize) == 0 {
            (*z).c = c1;
            break;
        } else {
            let mut c2: ::core::ffi::c_int = (*z).c;
            let mut ret: ::core::ffi::c_int = r_remove_tense_suffix(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            (*z).c = c2;
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_tense_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut ret: ::core::ffi::c_int = r_has_min_length(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_25 as *const among,
        46 as ::core::ffi::c_int,
    );
    if !(among_var == 0) {
        (*z).bra = (*z).c;
        match among_var {
            1 => {
                let mut ret_0: ::core::ffi::c_int = slice_del(z);
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
                current_block = 2122094917359643297;
            }
            2 => {
                let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                        != 4 as ::core::ffi::c_int
                    || 1951712 as ::core::ffi::c_int
                        >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                            as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                        & 1 as ::core::ffi::c_int == 0
                {
                    current_block = 14856155306599981950;
                } else if find_among_b(
                    z,
                    &raw const a_23 as *const among,
                    12 as ::core::ffi::c_int,
                ) == 0
                {
                    current_block = 14856155306599981950;
                } else {
                    current_block = 10346081849507595293;
                }
                match current_block {
                    10346081849507595293 => {}
                    _ => {
                        (*z).c = (*z).l - m3;
                        let mut ret_1: ::core::ffi::c_int = slice_del(z);
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                        current_block = 2122094917359643297;
                    }
                }
            }
            3 => {
                let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                if find_among_b(
                    z,
                    &raw const a_24 as *const among,
                    8 as ::core::ffi::c_int,
                ) == 0
                {
                    (*z).c = (*z).l - m4;
                    let mut ret_2: ::core::ffi::c_int = slice_del(z);
                    if ret_2 < 0 as ::core::ffi::c_int {
                        return ret_2;
                    }
                    current_block = 2122094917359643297;
                } else {
                    current_block = 10346081849507595293;
                }
            }
            4 => {
                let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
                if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_37 as *const symbol)
                    == 0
                {
                    (*z).c = (*z).l - m5;
                    let mut ret_3: ::core::ffi::c_int = slice_from_s(
                        z,
                        3 as ::core::ffi::c_int,
                        &raw const s_38 as *const symbol,
                    );
                    if ret_3 < 0 as ::core::ffi::c_int {
                        return ret_3;
                    }
                    current_block = 2122094917359643297;
                } else {
                    current_block = 10346081849507595293;
                }
            }
            5 => {
                let mut ret_4: ::core::ffi::c_int = slice_from_s(
                    z,
                    3 as ::core::ffi::c_int,
                    &raw const s_39 as *const symbol,
                );
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
                current_block = 2122094917359643297;
            }
            6 => {
                let mut m_test6: ::core::ffi::c_int = (*z).l - (*z).c;
                if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_40 as *const symbol)
                    == 0
                {
                    current_block = 10346081849507595293;
                } else {
                    (*z).c = (*z).l - m_test6;
                    let mut ret_5: ::core::ffi::c_int = slice_del(z);
                    if ret_5 < 0 as ::core::ffi::c_int {
                        return ret_5;
                    }
                    current_block = 2122094917359643297;
                }
            }
            _ => {
                current_block = 2122094917359643297;
            }
        }
        match current_block {
            10346081849507595293 => {}
            _ => {
                *(*z).I.offset(1 as ::core::ffi::c_int as isize) = 1
                    as ::core::ffi::c_int;
                (*z).c = (*z).l - m_test2;
            }
        }
    }
    (*z).c = (*z).l - m1;
    let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !((*z).c - 8 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 141 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 177 as ::core::ffi::c_int)
    {
        if !(find_among_b(z, &raw const a_26 as *const among, 6 as ::core::ffi::c_int)
            == 0)
        {
            (*z).bra = (*z).c;
            let mut ret_6: ::core::ffi::c_int = slice_del(z);
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
        }
    }
    (*z).c = (*z).l - m7;
    (*z).c = (*z).lb;
    let mut ret_7: ::core::ffi::c_int = r_fix_endings(z);
    if ret_7 < 0 as ::core::ffi::c_int {
        return ret_7;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn tamil_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_fix_ending(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = c1;
    let mut ret_0: ::core::ffi::c_int = r_has_min_length(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    let mut c2: ::core::ffi::c_int = (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_remove_question_prefixes(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = c2;
    let mut c3: ::core::ffi::c_int = (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_remove_pronoun_prefixes(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = c3;
    let mut c4: ::core::ffi::c_int = (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_remove_question_suffixes(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = c4;
    let mut c5: ::core::ffi::c_int = (*z).c;
    let mut ret_4: ::core::ffi::c_int = r_remove_um(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    (*z).c = c5;
    let mut c6: ::core::ffi::c_int = (*z).c;
    let mut ret_5: ::core::ffi::c_int = r_remove_common_word_endings(z);
    if ret_5 < 0 as ::core::ffi::c_int {
        return ret_5;
    }
    (*z).c = c6;
    let mut c7: ::core::ffi::c_int = (*z).c;
    let mut ret_6: ::core::ffi::c_int = r_remove_vetrumai_urupukal(z);
    if ret_6 < 0 as ::core::ffi::c_int {
        return ret_6;
    }
    (*z).c = c7;
    let mut c8: ::core::ffi::c_int = (*z).c;
    let mut ret_7: ::core::ffi::c_int = r_remove_plural_suffix(z);
    if ret_7 < 0 as ::core::ffi::c_int {
        return ret_7;
    }
    (*z).c = c8;
    let mut c9: ::core::ffi::c_int = (*z).c;
    let mut ret_8: ::core::ffi::c_int = r_remove_command_suffixes(z);
    if ret_8 < 0 as ::core::ffi::c_int {
        return ret_8;
    }
    (*z).c = c9;
    let mut c10: ::core::ffi::c_int = (*z).c;
    let mut ret_9: ::core::ffi::c_int = r_remove_tense_suffixes(z);
    if ret_9 < 0 as ::core::ffi::c_int {
        return ret_9;
    }
    (*z).c = c10;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn tamil_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 2 as ::core::ffi::c_int);
}
pub unsafe fn tamil_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
