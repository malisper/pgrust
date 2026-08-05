use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 10] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_0_1: [symbol; 12] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_0_2: [symbol; 12] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_0_3: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_0_4: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_0_5: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_0_6: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_0_7: [symbol; 8] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_0_8: [symbol; 8] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 9] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
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
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_1: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_2: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_1_3: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_1_4: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_1_5: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_1_6: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_1_7: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_1_8: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_1_9: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_1_10: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_1_11: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_1_12: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_1_13: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_1_14: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_1_15: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_1_16: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_1_17: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_1_18: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_1_19: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_1_20: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_1_21: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_1_22: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_1_23: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_1_24: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_1_25: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 26] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_2_1: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_2_2: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_2_3: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_2_4: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_2_5: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_2_6: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_2_7: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 8] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_3_1: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut a_3: [among; 2] = unsafe {
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
    ]
};
static mut s_4_0: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_4_1: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_4_2: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_4_3: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_4_4: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_4_5: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_4_6: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_4_7: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_4_8: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_4_9: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_4_10: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_4_11: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_4_12: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_4_13: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_4_14: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_4_15: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_4_16: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_4_17: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_4_18: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_4_19: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_4_20: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_4_21: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_4_22: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_4_23: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_4_24: [symbol; 8] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_4_25: [symbol; 8] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_4_26: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_4_27: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_4_28: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_4_29: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_4_30: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_4_31: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_4_32: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_4_33: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_4_34: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_4_35: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_4_36: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_4_37: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_4_38: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_4_39: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_4_40: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_4_41: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_4_42: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_4_43: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_4_44: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_4_45: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 46] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_11 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_18 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_4_24 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_4_25 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_31 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_33 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_34 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_39 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_41 as *const symbol,
            substring_i: 40 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_42 as *const symbol,
            substring_i: 40 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_44 as *const symbol,
            substring_i: 43 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_4_45 as *const symbol,
            substring_i: 43 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_1: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_5_2: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_5_3: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_5_4: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_5_5: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_5_6: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_5_7: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_5_8: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_5_9: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_5_10: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_5_11: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_5_12: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_5_13: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_5_14: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_5_15: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_5_16: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_5_17: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_5_18: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_5_19: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_5_20: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_5_21: [symbol; 6] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_5_22: [symbol; 8] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_5_23: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_5_24: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_5_25: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_5_26: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_5_27: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_5_28: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_5_29: [symbol; 4] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_5_30: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_5_31: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_5_32: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_5_33: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_5_34: [symbol; 4] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_5_35: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut a_5: [among; 36] = unsafe {
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_8 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_11 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_17 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_20 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_21 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_5_22 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_23 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_25 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_27 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_28 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_5_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_5_33 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_6_1: [symbol; 8] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut a_6: [among; 2] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 6] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_7_1: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_7_2: [symbol; 8] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xd1 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_7_3: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_7: [among; 4] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 4] = [
    33 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    232 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_1: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_2: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_3: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_4: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_5: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_6: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_7: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_8: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_9: [symbol; 2] = [
    0xd1 as ::core::ffi::c_int as symbol,
    0x91 as ::core::ffi::c_int as symbol,
];
static mut s_10: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_11: [symbol; 2] = [
    0xd0 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = out_grouping_U(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        1072 as ::core::ffi::c_int,
        1103 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret < 0 as ::core::ffi::c_int) {
        (*z).c += ret;
        *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
        let mut ret_0: ::core::ffi::c_int = in_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            1072 as ::core::ffi::c_int,
            1103 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_0 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_0;
            let mut ret_1: ::core::ffi::c_int = out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                1072 as ::core::ffi::c_int,
                1103 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_1 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_1;
                let mut ret_2: ::core::ffi::c_int = in_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    1072 as ::core::ffi::c_int,
                    1103 as ::core::ffi::c_int,
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
    among_var = find_among_b(z, &raw const a_0 as *const among, 9 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_0 as *const symbol) == 0 {
                (*z).c = (*z).l - m1;
                if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_1 as *const symbol)
                    == 0
                {
                    return 0 as ::core::ffi::c_int;
                }
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
    among_var = find_among_b(z, &raw const a_2 as *const among, 8 as ::core::ffi::c_int);
    if among_var == 0 {
        (*z).c = (*z).l - m1;
    } else {
        (*z).bra = (*z).c;
        match among_var {
            1 => {
                current_block = 8515828400728868193;
                match current_block {
                    15165650403497571337 => {
                        let mut ret_1: ::core::ffi::c_int = slice_del(z);
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                    }
                    _ => {
                        let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if eq_s_b(
                            z,
                            2 as ::core::ffi::c_int,
                            &raw const s_2 as *const symbol,
                        ) == 0
                        {
                            (*z).c = (*z).l - m2;
                            if eq_s_b(
                                z,
                                2 as ::core::ffi::c_int,
                                &raw const s_3 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m1;
                                current_block = 11307063007268554308;
                            } else {
                                current_block = 1131184065681666404;
                            }
                        } else {
                            current_block = 1131184065681666404;
                        }
                        match current_block {
                            11307063007268554308 => {}
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
                current_block = 15165650403497571337;
                match current_block {
                    15165650403497571337 => {
                        let mut ret_1: ::core::ffi::c_int = slice_del(z);
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                    }
                    _ => {
                        let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
                        if eq_s_b(
                            z,
                            2 as ::core::ffi::c_int,
                            &raw const s_2 as *const symbol,
                        ) == 0
                        {
                            (*z).c = (*z).l - m2;
                            if eq_s_b(
                                z,
                                2 as ::core::ffi::c_int,
                                &raw const s_3 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m1;
                                current_block = 11307063007268554308;
                            } else {
                                current_block = 1131184065681666404;
                            }
                        } else {
                            current_block = 1131184065681666404;
                        }
                        match current_block {
                            11307063007268554308 => {}
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
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_reflexive(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 140 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 143 as ::core::ffi::c_int
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
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_4 as *const symbol) == 0 {
                (*z).c = (*z).l - m1;
                if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_5 as *const symbol)
                    == 0
                {
                    return 0 as ::core::ffi::c_int;
                }
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
    if (*z).c - 5 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 130 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 140 as ::core::ffi::c_int
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
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_6 as *const symbol) == 0 {
                return 0 as ::core::ffi::c_int;
            }
            (*z).bra = (*z).c;
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_7 as *const symbol) == 0 {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_8 as *const symbol) == 0 {
                return 0 as ::core::ffi::c_int;
            }
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
pub unsafe fn russian_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut current_block_8: u64;
    loop {
        let mut c2: ::core::ffi::c_int = (*z).c;
        loop {
            let mut c3: ::core::ffi::c_int = (*z).c;
            (*z).bra = (*z).c;
            if eq_s(z, 2 as ::core::ffi::c_int, &raw const s_9 as *const symbol) == 0 {
                (*z).c = c3;
                let mut ret: ::core::ffi::c_int = skip_utf8(
                    (*z).p,
                    (*z).c,
                    (*z).l,
                    1 as ::core::ffi::c_int,
                );
                if ret < 0 as ::core::ffi::c_int {
                    current_block_8 = 1308269956193026931;
                    break;
                }
                (*z).c = ret;
            } else {
                (*z).ket = (*z).c;
                (*z).c = c3;
                current_block_8 = 13109137661213826276;
                break;
            }
        }
        match current_block_8 {
            13109137661213826276 => {
                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_10 as *const symbol,
                );
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
            }
            _ => {
                (*z).c = c2;
                break;
            }
        }
    }
    (*z).c = c1;
    let mut ret_1: ::core::ffi::c_int = r_mark_regions(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
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
    let mut ret_2: ::core::ffi::c_int = r_perfective_gerund(z);
    if ret_2 == 0 as ::core::ffi::c_int {
        (*z).c = (*z).l - m6;
        let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_3: ::core::ffi::c_int = r_reflexive(z);
        if ret_3 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m7;
        } else if ret_3 < 0 as ::core::ffi::c_int {
            return ret_3
        }
        let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_4: ::core::ffi::c_int = r_adjectival(z);
        if ret_4 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m8;
            let mut ret_5: ::core::ffi::c_int = r_verb(z);
            if ret_5 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m8;
                let mut ret_6: ::core::ffi::c_int = r_noun(z);
                if !(ret_6 == 0 as ::core::ffi::c_int) {
                    if ret_6 < 0 as ::core::ffi::c_int {
                        return ret_6;
                    }
                }
            } else if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5
            }
        } else if ret_4 < 0 as ::core::ffi::c_int {
            return ret_4
        }
    } else if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2
    }
    (*z).c = (*z).l - m5;
    let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_11 as *const symbol) == 0 {
        (*z).c = (*z).l - m9;
    } else {
        (*z).bra = (*z).c;
        let mut ret_7: ::core::ffi::c_int = slice_del(z);
        if ret_7 < 0 as ::core::ffi::c_int {
            return ret_7;
        }
    }
    let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_8: ::core::ffi::c_int = r_derivational(z);
    if ret_8 < 0 as ::core::ffi::c_int {
        return ret_8;
    }
    (*z).c = (*z).l - m10;
    let mut m11: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_9: ::core::ffi::c_int = r_tidy_up(z);
    if ret_9 < 0 as ::core::ffi::c_int {
        return ret_9;
    }
    (*z).c = (*z).l - m11;
    (*z).lb = mlimit4;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn russian_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 2 as ::core::ffi::c_int);
}
pub unsafe fn russian_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
