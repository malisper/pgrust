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
    0xcf as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_0_2: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_0_3: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_0_4: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_0_5: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_0_6: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_0_7: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_0_8: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_0_9: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_0_10: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_0_11: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_0_12: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_0_13: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_0_14: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x90 as ::core::ffi::c_int as symbol,
];
static mut s_0_15: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x91 as ::core::ffi::c_int as symbol,
];
static mut s_0_16: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x92 as ::core::ffi::c_int as symbol,
];
static mut s_0_17: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x93 as ::core::ffi::c_int as symbol,
];
static mut s_0_18: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x94 as ::core::ffi::c_int as symbol,
];
static mut s_0_19: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_0_20: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x96 as ::core::ffi::c_int as symbol,
];
static mut s_0_21: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
];
static mut s_0_22: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x98 as ::core::ffi::c_int as symbol,
];
static mut s_0_23: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
];
static mut s_0_24: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_0_25: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_0_26: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x9c as ::core::ffi::c_int as symbol,
];
static mut s_0_27: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x9d as ::core::ffi::c_int as symbol,
];
static mut s_0_28: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x9e as ::core::ffi::c_int as symbol,
];
static mut s_0_29: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_0_30: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_0_31: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_0_32: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_0_33: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_0_34: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_0_35: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
];
static mut s_0_36: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_0_37: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_0_38: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_0_39: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_0_40: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_0_41: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_0_42: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
];
static mut s_0_43: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
];
static mut s_0_44: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_0_45: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 46] = unsafe {
    [
        among {
            s_size: 0 as ::core::ffi::c_int,
            s: ::core::ptr::null::<symbol>(),
            substring_i: -(1 as ::core::ffi::c_int),
            result: 25 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 20 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 20 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_11 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 20 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_12 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 24 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_13 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 24 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_14 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_15 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_16 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_17 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_18 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_19 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_20 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_21 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_22 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_23 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_24 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_25 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_26 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_27 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_28 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_29 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_30 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_31 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_32 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_33 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 19 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_34 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 20 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_35 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 21 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_36 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 22 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_37 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 23 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_38 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 24 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_39 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_40 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 20 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_41 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_42 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_43 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_44 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_45 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 20 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_1: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_2: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_3: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_4: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_5: [symbol; 20] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_6: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_7: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_8: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_9: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_10: [symbol; 18] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_11: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_1_12: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_1_13: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_1_14: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_1_15: [symbol; 16] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_1_16: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_1_17: [symbol; 18] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_18: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_19: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_20: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_21: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_22: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_23: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_24: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_25: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_26: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_27: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1_28: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
];
static mut s_1_29: [symbol; 20] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_30: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_31: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_32: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_33: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_34: [symbol; 18] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_35: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_36: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_37: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_38: [symbol; 16] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_1_39: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 40] = unsafe {
    [
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 20 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_1_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_1_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_1_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_1_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 20 as ::core::ffi::c_int,
            s: &raw const s_1_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_1_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_1_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_1_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_1_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_1_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_2_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_2_2: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_3: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_4: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_5: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_6: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_7: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_8: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_9: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_10: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_11: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_12: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_13: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_2_14: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_2_15: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_2_16: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_2_17: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_2_18: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_2_19: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_2_20: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_2_21: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_2_22: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_2_23: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_2_24: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_2_25: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_2_26: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_2_27: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_2_28: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_2_29: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_2_30: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 31] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_2_11 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_2_12 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_2_13 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_2_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_17 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_2_18 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_2_19 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_20 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
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
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_2_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_2_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
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
    ]
};
static mut s_3_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_3_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_3_2: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_3_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_3_4: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_3_5: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_3_6: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_3_7: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_3_8: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_3_9: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_3_10: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_3_11: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_3_12: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_3_13: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_3: [among; 14] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
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
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_3_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_3_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_3_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_4_1: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_4_2: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_4_3: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_4_4: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_4_5: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_4_6: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_4_7: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 8] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_5_1: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_5_2: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_5_3: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_5_4: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_5_5: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_5_6: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_5: [among; 7] = unsafe {
    [
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_5_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_5_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_5_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_5_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_5_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_5_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_6_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_6_2: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_6_3: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_6_4: [symbol; 18] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_6_5: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_6_6: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_6_7: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_6_8: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_6_9: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_6_10: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_6_11: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_6_12: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_13: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_14: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_15: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_16: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_17: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_18: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_19: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_20: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_21: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_22: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_23: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_6_24: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_6_25: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_6_26: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_6_27: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_6_28: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_6_29: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_6_30: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_6_31: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_6: [among; 32] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_6_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_6_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_6_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_6_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_6_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_6_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_6_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_6_14 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_6_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_6_20 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_6_21 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_6_22 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_6_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_6_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_27 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_6_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_6_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_7_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_7_2: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_7_3: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_7_4: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_7_5: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_7_6: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_7: [among; 7] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_7_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_7_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_7_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_7_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_8_0: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_8_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_8_2: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_8_3: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_8_4: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_8_5: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_8_6: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_8_7: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_8: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_9: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_10: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_11: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_12: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_13: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_14: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_15: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_16: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_8_17: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_8_18: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_8: [among; 19] = unsafe {
    [
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_8_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_8_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_8_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_8_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_8_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_8_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_8_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_8_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_8_12 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_8_13 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_8_14 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_8_15 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_8_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_8_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_8_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_9_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_9_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_9_2: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_9_3: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_9_4: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_9_5: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_9_6: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_9: [among; 7] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_9_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_9_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_9_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_9_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_9_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_9_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_9_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_10_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_10_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_10_2: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_10_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_10_4: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_10_5: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_10_6: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_10_7: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_10_8: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_10_9: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_10_10: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_10_11: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_10_12: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_10_13: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_10_14: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_10_15: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_10_16: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_10_17: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_10_18: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_10_19: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_10_20: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_10_21: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_10_22: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_10_23: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_10_24: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_10_25: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_10_26: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_10_27: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_10_28: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_10_29: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_10_30: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_10_31: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_10_32: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_10_33: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_10_34: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_10_35: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_10_36: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_10_37: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_10_38: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_10_39: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_10: [among; 40] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_12 as *const symbol,
            substring_i: 11 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_14 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_24 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_10_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_10_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_37 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_10_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_11_0: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_11_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_11_2: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_11_3: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_11_4: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_11_5: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_11_6: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_11_7: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
];
static mut s_11_8: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_11_9: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_11_10: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_11: [among; 11] = unsafe {
    [
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_11_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_11_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_11_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_12_0: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_12_1: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_12_2: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_12_3: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_12_4: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_12_5: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_12_6: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_12: [among; 7] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_12_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_12_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_12_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_12_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_12_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_12_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_12_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_13_0: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_13_1: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_13_2: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_13_3: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_13_4: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_13_5: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_13_6: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_13_7: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_13_8: [symbol; 20] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_13_9: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_13: [among; 10] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_13_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_13_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_13_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_13_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_13_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_13_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_13_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_13_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 20 as ::core::ffi::c_int,
            s: &raw const s_13_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_13_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_14_0: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_14_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_14_2: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_14_3: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_14_4: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_14_5: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_14: [among; 6] = unsafe {
    [
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_14_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_14_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_14_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_14_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_14_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_14_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_15_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_15_1: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut a_15: [among; 2] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_15_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_15_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_16_0: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_16_1: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_16_2: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_16_3: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut a_16: [among; 4] = unsafe {
    [
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_16_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_16_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_16_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_16_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_17_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_17_1: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_17_2: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_3: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_4: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_5: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_6: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_7: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_8: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_9: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17_10: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_17_11: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_17_12: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_17_13: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_17_14: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_17_15: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_17_16: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_17_17: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_17_18: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_17_19: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_17_20: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_17_21: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_17_22: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_17_23: [symbol; 18] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_17_24: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_17_25: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_17_26: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_17_27: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_17_28: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_17_29: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_17_30: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_17_31: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_17_32: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_17_33: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_17_34: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_17_35: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_17_36: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_17_37: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_17_38: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_17_39: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_17_40: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_17_41: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_17_42: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_17_43: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_17_44: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_17_45: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_17: [among; 46] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_5 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_17_6 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_7 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_8 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_9 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_12 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_17_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_17_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_18 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_21 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_22 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_17_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_28 as *const symbol,
            substring_i: 27 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_17_29 as *const symbol,
            substring_i: 27 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_30 as *const symbol,
            substring_i: 27 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_17_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_17_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_17_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_18_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_18_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_18_2: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_18_3: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_18_4: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_18_5: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_18_6: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_18_7: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_18: [among; 8] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_18_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_18_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_18_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_18_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_18_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_18_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_18_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_18_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_19_0: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_19_1: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_19_2: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_19_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_19: [among; 4] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_19_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_19_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_19_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_19_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_20_0: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_20_1: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_20: [among; 2] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_20_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_20_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_21_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_21_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_21_2: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_21: [among; 3] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_21_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_21_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_21_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_22_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_22_1: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_22_2: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_22_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_22_4: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_22_5: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_22_6: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_22: [among; 7] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_22_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_22_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_22_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_22_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_22_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_22_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_22_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_23_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_23_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_23_2: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_23_3: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_23: [among; 4] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_23_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_23_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_23_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_23_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_24_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_24_1: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_24: [among; 2] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_24_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_24_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_25_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_25_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_25_2: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_25_3: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_25_4: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_25_5: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_25_6: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_25_7: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_25_8: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_25_9: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_25: [among; 10] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_25_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_25_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_25_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_25_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_25_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_25_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_25_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_25_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_25_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_25_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_26_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_26_1: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_26: [among; 2] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_26_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_26_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_27_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_27_1: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_27_2: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_27_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_27_4: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_27_5: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_27_6: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_27_7: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut a_27: [among; 8] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_27_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_27_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_27_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_27_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_27_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_27_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_27_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_27_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_28_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_28_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_28: [among; 2] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_28_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_28_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_29_0: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_29_1: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_29_2: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_29_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_29_4: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_29_5: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_29_6: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_29_7: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_29_8: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_29_9: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_29_10: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_29_11: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_29_12: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_29_13: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_29_14: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut a_29: [among; 15] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_29_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_29_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_29_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_29_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_29_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_29_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_29_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_29_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_29_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_29_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_29_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_29_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_29_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_29_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_29_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_30_0: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_30_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_30: [among; 2] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_30_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_30_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_31_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_31_1: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_31_2: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_31_3: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_31_4: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_31_5: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_31_6: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_31_7: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_31: [among; 8] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_31_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_31_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_31_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_31_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_31_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_31_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_31_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_31_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_32_0: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_32_1: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_32_2: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_32: [among; 3] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_32_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_32_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_32_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_33_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_33_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_33_2: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_33_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_33: [among; 4] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_33_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_33_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_33_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_33_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_34_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_34_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_34_2: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_34_3: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_34_4: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_34_5: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_34_6: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_34_7: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_34_8: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_34_9: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_34_10: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_34_11: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_34_12: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_34_13: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_34_14: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_34_15: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_34_16: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_34_17: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_34_18: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_34_19: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_34_20: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_34_21: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_34_22: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_34_23: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_34_24: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_34_25: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_34_26: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_34_27: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_34_28: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_34_29: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_34_30: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_34_31: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_34_32: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_34_33: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_34_34: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_34_35: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_34: [among; 36] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_34_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_34_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_34_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_34_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_34_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_34_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_34_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_34_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_34_14 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_34_15 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_34_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_34_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_34_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_34_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_34_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_22 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_34_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_34_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_34_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_34_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_34_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_34_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_34_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_34_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_35_0: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_35_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_35_2: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_35_3: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_35_4: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_35: [among; 5] = unsafe {
    [
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_35_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_35_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_35_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_35_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_35_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_36_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_36_1: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_36_2: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_36_3: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_36_4: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_36_5: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_36_6: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_36_7: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_36_8: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_36_9: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_36_10: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_36_11: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut a_36: [among; 12] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_36_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_36_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_36_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_36_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_36_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_36_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_36_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_36_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_36_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_36_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_36_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_36_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_37_0: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_37_1: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut a_37: [among; 2] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_37_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_37_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_38_0: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_2: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_3: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_4: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_5: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_6: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_7: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_8: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_9: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_38_10: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_38: [among; 11] = unsafe {
    [
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_38_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_38_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_38_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_38_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_38_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_38_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_38_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_38_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_38_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_38_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_38_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_39_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_39_1: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_39_2: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_39_3: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_39_4: [symbol; 18] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_39_5: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_39_6: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_39_7: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_39_8: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_39_9: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_39_10: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_39_11: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_39_12: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_39_13: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_39_14: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_39_15: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_39_16: [symbol; 16] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_39_17: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_39_18: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_39_19: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_39_20: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_39_21: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_39_22: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_39_23: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_39_24: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_39_25: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_39_26: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_39_27: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_39_28: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_39_29: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_39_30: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_39_31: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_39_32: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_39_33: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_39_34: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_39_35: [symbol; 16] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_39_36: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_39_37: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_39_38: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_39_39: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_39_40: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_39_41: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_39_42: [symbol; 22] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_39_43: [symbol; 22] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_39_44: [symbol; 22] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_39_45: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_39_46: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_39_47: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_39_48: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_39_49: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_39_50: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_39_51: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_39_52: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_53: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_54: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_55: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_56: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_57: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_58: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_59: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_60: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_61: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_62: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_63: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_39_64: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_39_65: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_39_66: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_39_67: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_39_68: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_39_69: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_39_70: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_39_71: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_39_72: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_39_73: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_39_74: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_39_75: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_39_76: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_77: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_78: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_79: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_80: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_81: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_82: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_83: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_84: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_85: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_86: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_87: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_39_88: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_39_89: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_39_90: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_39_91: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_39_92: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_39_93: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_39_94: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_39: [among; 95] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_39_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_39_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_39_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_39_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_12 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_39_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_39_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_17 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_18 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_19 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_39_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_31 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_32 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_33 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_34 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_39_35 as *const symbol,
            substring_i: 34 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_36 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_37 as *const symbol,
            substring_i: 36 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_39_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_39 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_40 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_41 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 22 as ::core::ffi::c_int,
            s: &raw const s_39_42 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 22 as ::core::ffi::c_int,
            s: &raw const s_39_43 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 22 as ::core::ffi::c_int,
            s: &raw const s_39_44 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_45 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_46 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_47 as *const symbol,
            substring_i: 46 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_48 as *const symbol,
            substring_i: 46 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_50 as *const symbol,
            substring_i: 49 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_39_51 as *const symbol,
            substring_i: 50 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_39_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_53 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_54 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_39_55 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_56 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_57 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_58 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_59 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_60 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_61 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_62 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_63 as *const symbol,
            substring_i: 52 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_64 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_39_65 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_66 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_67 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_68 as *const symbol,
            substring_i: 67 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_69 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_70 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_71 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_72 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_39_73 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_74 as *const symbol,
            substring_i: 73 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_75 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_39_76 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_39_77 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_78 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_79 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_80 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_81 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_82 as *const symbol,
            substring_i: 81 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_83 as *const symbol,
            substring_i: 81 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_84 as *const symbol,
            substring_i: 81 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_85 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_39_86 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_39_87 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_39_88 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_39_89 as *const symbol,
            substring_i: 88 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_39_90 as *const symbol,
            substring_i: 88 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_39_91 as *const symbol,
            substring_i: 88 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_39_92 as *const symbol,
            substring_i: 88 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_39_93 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_39_94 as *const symbol,
            substring_i: 93 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_40_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_40: [among; 1] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_40_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_41_0: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_41_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_41_2: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_41_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_41_4: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_41_5: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_41_6: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_41_7: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_41_8: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_41_9: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_41_10: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_41_11: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_41_12: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_13: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_14: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_15: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_16: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_17: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_18: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_19: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_20: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_21: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_22: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_23: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_41_24: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_41_25: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_41_26: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_41_27: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_41_28: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_41_29: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_41_30: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_41: [among; 31] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_41_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_41_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_41_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_41_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_41_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_41_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_41_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_41_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_41_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_41_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_42_0: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_42_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_42_2: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_42_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_42_4: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_42_5: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_42_6: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_42_7: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_42_8: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_42_9: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_42_10: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_42_11: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_42_12: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_42_13: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_42_14: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_42_15: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_42_16: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_42_17: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_42_18: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_42_19: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_42_20: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_42_21: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_42_22: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_42_23: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_42_24: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_42: [among; 25] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_42_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_42_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_42_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_42_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_42_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_42_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_42_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_42_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_42_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_42_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_42_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_42_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_42_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_42_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_42_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_42_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_42_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_42_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_42_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_42_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_42_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_42_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_42_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_42_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_42_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_43_0: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_43_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut a_43: [among; 2] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_43_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_43_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_44_0: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_44_1: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_44: [among; 2] = unsafe {
    [
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_44_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_44_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_45_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_45_1: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_45_2: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_45_3: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_45_4: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_45_5: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut a_45: [among; 6] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_45_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_45_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_45_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_45_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_45_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_45_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_46_0: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_46_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_46_2: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_46_3: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_46_4: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_46_5: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_46_6: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_46_7: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_46_8: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_46: [among; 9] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_46_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_46_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_46_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_46_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_46_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_46_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_46_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_46_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_46_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_47_0: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_47_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_47_2: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_47: [among; 3] = unsafe {
    [
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_47_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_47_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_47_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_48_0: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_48_1: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_48_2: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_48_3: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_48_4: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_48_5: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut a_48: [among; 6] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_48_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_48_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_48_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_48_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_48_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_48_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_49_0: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_49_1: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_49_2: [symbol; 18] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_49_3: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_49_4: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut a_49: [among; 5] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_49_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_49_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_49_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_49_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_49_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_50_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_50_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_50_2: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_50: [among; 3] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_50_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_50_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_50_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_51_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_51_1: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_51_2: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_51_3: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_51_4: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_51_5: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_51_6: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_51_7: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_51_8: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_51_9: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_51_10: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_51_11: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut a_51: [among; 12] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_51_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_51_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_51_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_51_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_51_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_51_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_51_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_51_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_51_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_51_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_51_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_51_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_52_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_52_1: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_52_2: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_52_3: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_52_4: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_52_5: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_52_6: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_52_7: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_52_8: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_52_9: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_52_10: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_52_11: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_52_12: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_52_13: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_52_14: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_52_15: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_52_16: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_52_17: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_52_18: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_52_19: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_52_20: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_52_21: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_52_22: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_52_23: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_52_24: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_52: [among; 25] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_52_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_52_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_52_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_52_3 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_52_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_52_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_52_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_52_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_52_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_52_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_52_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_52_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_52_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_52_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_52_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_52_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_52_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_52_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_52_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_52_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_52_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_52_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_52_22 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_52_23 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_52_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_53_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_53_1: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_53_2: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_53: [among; 3] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_53_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_53_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_53_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_54_0: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_54_1: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_54_2: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_54_3: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_54_4: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_54_5: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_54_6: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_54_7: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_54_8: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_54_9: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_54_10: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_54_11: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_54: [among; 12] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_54_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_54_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_54_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_54_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_54_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_54_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_54_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_54_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_54_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_54_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_54_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_54_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_55_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_2: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_3: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_4: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_5: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_6: [symbol; 16] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_7: [symbol; 14] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_8: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_9: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_10: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_11: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_55_12: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_55_13: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_55_14: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_55_15: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_55_16: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_55_17: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_55_18: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_55_19: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_20: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_21: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_22: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_23: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_24: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_25: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_26: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_27: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_28: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_29: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_30: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_55_31: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_55_32: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_55_33: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_55_34: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_55_35: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_55_36: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_55_37: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_55_38: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_55_39: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_55_40: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_55_41: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_55_42: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_55_43: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_55: [among; 44] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_55_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_55_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_55_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_55_6 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_55_7 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_55_8 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_9 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_55_10 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_11 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_55_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_55_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_55_14 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_55_15 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_55_16 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_18 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_55_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_55_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_55_21 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_55_22 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_55_23 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_24 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_25 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_26 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_27 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_28 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_29 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_55_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_55_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_55_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_55_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_55_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_55_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_55_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_55_39 as *const symbol,
            substring_i: 38 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_55_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_55_41 as *const symbol,
            substring_i: 40 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_55_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_55_43 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_56_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_56_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_56_2: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_56: [among; 3] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_56_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_56_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_56_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_57_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_57_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_57_2: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_57: [among; 3] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_57_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_57_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_57_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_58_0: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_58_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_58_2: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_58_3: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_58_4: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_58_5: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_58: [among; 6] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_58_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_58_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_58_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_58_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_58_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_58_5 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_59_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_59: [among; 1] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_59_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_60_0: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_60_1: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_60_2: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_60_3: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_60_4: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_60_5: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_60_6: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_60_7: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_60_8: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_60_9: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_60: [among; 10] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_60_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_60_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_60_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_60_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_60_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_60_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_60_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_60_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_60_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_60_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_61_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_61_1: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_61_2: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_61: [among; 3] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_61_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_61_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_61_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_62_0: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_62_1: [symbol; 22] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_62_2: [symbol; 18] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_62_3: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_62_4: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_62_5: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_62: [among; 6] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_62_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 22 as ::core::ffi::c_int,
            s: &raw const s_62_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_62_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_62_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_62_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_62_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_63_0: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_63_1: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_63_2: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut a_63: [among; 3] = unsafe {
    [
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_63_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_63_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_63_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_64_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_64_1: [symbol; 16] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_64_2: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_64_3: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_64_4: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_64_5: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_64_6: [symbol; 12] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut a_64: [among; 7] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_64_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_64_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_64_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_64_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_64_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_64_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_64_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_65_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_65_1: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_65_2: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut a_65: [among; 3] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_65_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_65_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_65_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_66_0: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_1: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_2: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_3: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_4: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_5: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_6: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_7: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_8: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_9: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_66_10: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_66_11: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_66_12: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_66_13: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_66_14: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_66_15: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_66_16: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_66_17: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_66_18: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_66_19: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_66_20: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_66_21: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_66_22: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_23: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_24: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_25: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_26: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_27: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_28: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_29: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_30: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_31: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_32: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_33: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_34: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_35: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_66_36: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
];
static mut s_66_37: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_38: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_39: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_40: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_41: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_42: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_43: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_44: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_45: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_46: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_47: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_48: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_49: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_50: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_51: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_52: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_53: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_54: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_55: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_66_56: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_57: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_58: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_59: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_60: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_61: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_62: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_63: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_64: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_65: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_66: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_67: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_68: [symbol; 18] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_69: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_70: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_71: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_72: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_73: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_74: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_75: [symbol; 14] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_76: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_77: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_78: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_79: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_80: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_81: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_82: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_66_83: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut a_66: [among; 84] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_66_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_5 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_66_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_66_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_66_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_66_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_14 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_66_15 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_66_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_18 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_20 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_21 as *const symbol,
            substring_i: 20 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_66_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_24 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_25 as *const symbol,
            substring_i: 24 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_26 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_66_27 as *const symbol,
            substring_i: 26 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_28 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_29 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_30 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_31 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_32 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_33 as *const symbol,
            substring_i: 32 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_34 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_35 as *const symbol,
            substring_i: 34 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_66_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_66_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_38 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_39 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_40 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_41 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_42 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_43 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_44 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_45 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_46 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_47 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_48 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_49 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_50 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_51 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_52 as *const symbol,
            substring_i: 51 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_66_53 as *const symbol,
            substring_i: 51 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_54 as *const symbol,
            substring_i: 51 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_55 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_66_56 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_57 as *const symbol,
            substring_i: 56 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_58 as *const symbol,
            substring_i: 56 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_59 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_60 as *const symbol,
            substring_i: 56 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_61 as *const symbol,
            substring_i: 56 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_62 as *const symbol,
            substring_i: 61 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_63 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_64 as *const symbol,
            substring_i: 63 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_66_65 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_66 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_66_67 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 18 as ::core::ffi::c_int,
            s: &raw const s_66_68 as *const symbol,
            substring_i: 67 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_69 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_70 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_66_71 as *const symbol,
            substring_i: 70 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_72 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 16 as ::core::ffi::c_int,
            s: &raw const s_66_73 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_74 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 14 as ::core::ffi::c_int,
            s: &raw const s_66_75 as *const symbol,
            substring_i: 74 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_76 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_77 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_78 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_66_79 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_80 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_66_81 as *const symbol,
            substring_i: 65 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 12 as ::core::ffi::c_int,
            s: &raw const s_66_82 as *const symbol,
            substring_i: 81 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_66_83 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_67_0: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_67_1: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_67_2: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_67_3: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_67_4: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_67_5: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_67_6: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_67_7: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut a_67: [among; 8] = unsafe {
    [
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_67_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_67_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_67_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_67_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 10 as ::core::ffi::c_int,
            s: &raw const s_67_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_67_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_67_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_67_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 4] = [
    81 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_v2: [::core::ffi::c_uchar; 4] = [
    81 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_2: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_3: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_4: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_5: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_6: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
];
static mut s_7: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_8: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_9: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_10: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_11: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_12: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_13: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_14: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_15: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_16: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_17: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_18: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_19: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_20: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_21: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_22: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_23: [symbol; 2] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_24: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_25: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_26: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_27: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_28: [symbol; 8] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_29: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_30: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_31: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_32: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_33: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_34: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_35: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_36: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_37: [symbol; 4] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_38: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_39: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_40: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_41: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_42: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_43: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_44: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_45: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_46: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_47: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_48: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_49: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_50: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_51: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_52: [symbol; 10] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_53: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_54: [symbol; 16] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_55: [symbol; 12] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_56: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_57: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_58: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_59: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_60: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_61: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_62: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_63: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_64: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_65: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_66: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_67: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_68: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_69: [symbol; 2] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_70: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_71: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_72: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_73: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_74: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_75: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_76: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_77: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_78: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_79: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_80: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_81: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_82: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_83: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_84: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_85: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_86: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_87: [symbol; 6] = [
    0xcf as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_88: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_89: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_90: [symbol; 10] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_91: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_92: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_93: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_94: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_95: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_96: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_97: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_98: [symbol; 8] = [
    0xce as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_99: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_100: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_101: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_102: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_103: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_104: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_105: [symbol; 6] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
    0xcf as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_106: [symbol; 4] = [
    0xce as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xce as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
unsafe fn r_has_min_length(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (len_utf8((*z).p) >= 3 as ::core::ffi::c_int) as ::core::ffi::c_int;
}
unsafe fn r_tolower(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    loop {
        let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
        (*z).ket = (*z).c;
        among_var = find_among_b(
            z,
            &raw const a_0 as *const among,
            46 as ::core::ffi::c_int,
        );
        (*z).bra = (*z).c;
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
            3 => {
                let mut ret_1: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_2 as *const symbol,
                );
                if ret_1 < 0 as ::core::ffi::c_int {
                    return ret_1;
                }
            }
            4 => {
                let mut ret_2: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_3 as *const symbol,
                );
                if ret_2 < 0 as ::core::ffi::c_int {
                    return ret_2;
                }
            }
            5 => {
                let mut ret_3: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
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
                let mut ret_5: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_6 as *const symbol,
                );
                if ret_5 < 0 as ::core::ffi::c_int {
                    return ret_5;
                }
            }
            8 => {
                let mut ret_6: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_7 as *const symbol,
                );
                if ret_6 < 0 as ::core::ffi::c_int {
                    return ret_6;
                }
            }
            9 => {
                let mut ret_7: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_8 as *const symbol,
                );
                if ret_7 < 0 as ::core::ffi::c_int {
                    return ret_7;
                }
            }
            10 => {
                let mut ret_8: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_9 as *const symbol,
                );
                if ret_8 < 0 as ::core::ffi::c_int {
                    return ret_8;
                }
            }
            11 => {
                let mut ret_9: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_10 as *const symbol,
                );
                if ret_9 < 0 as ::core::ffi::c_int {
                    return ret_9;
                }
            }
            12 => {
                let mut ret_10: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_11 as *const symbol,
                );
                if ret_10 < 0 as ::core::ffi::c_int {
                    return ret_10;
                }
            }
            13 => {
                let mut ret_11: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_12 as *const symbol,
                );
                if ret_11 < 0 as ::core::ffi::c_int {
                    return ret_11;
                }
            }
            14 => {
                let mut ret_12: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_13 as *const symbol,
                );
                if ret_12 < 0 as ::core::ffi::c_int {
                    return ret_12;
                }
            }
            15 => {
                let mut ret_13: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_14 as *const symbol,
                );
                if ret_13 < 0 as ::core::ffi::c_int {
                    return ret_13;
                }
            }
            16 => {
                let mut ret_14: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_15 as *const symbol,
                );
                if ret_14 < 0 as ::core::ffi::c_int {
                    return ret_14;
                }
            }
            17 => {
                let mut ret_15: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_16 as *const symbol,
                );
                if ret_15 < 0 as ::core::ffi::c_int {
                    return ret_15;
                }
            }
            18 => {
                let mut ret_16: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_17 as *const symbol,
                );
                if ret_16 < 0 as ::core::ffi::c_int {
                    return ret_16;
                }
            }
            19 => {
                let mut ret_17: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_18 as *const symbol,
                );
                if ret_17 < 0 as ::core::ffi::c_int {
                    return ret_17;
                }
            }
            20 => {
                let mut ret_18: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_19 as *const symbol,
                );
                if ret_18 < 0 as ::core::ffi::c_int {
                    return ret_18;
                }
            }
            21 => {
                let mut ret_19: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_20 as *const symbol,
                );
                if ret_19 < 0 as ::core::ffi::c_int {
                    return ret_19;
                }
            }
            22 => {
                let mut ret_20: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_21 as *const symbol,
                );
                if ret_20 < 0 as ::core::ffi::c_int {
                    return ret_20;
                }
            }
            23 => {
                let mut ret_21: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_22 as *const symbol,
                );
                if ret_21 < 0 as ::core::ffi::c_int {
                    return ret_21;
                }
            }
            24 => {
                let mut ret_22: ::core::ffi::c_int = slice_from_s(
                    z,
                    2 as ::core::ffi::c_int,
                    &raw const s_23 as *const symbol,
                );
                if ret_22 < 0 as ::core::ffi::c_int {
                    return ret_22;
                }
            }
            25 => {
                let mut ret_23: ::core::ffi::c_int = skip_b_utf8(
                    (*z).p,
                    (*z).c,
                    (*z).lb,
                    1 as ::core::ffi::c_int,
                );
                if ret_23 < 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m1;
                    break;
                } else {
                    (*z).c = ret_23;
                }
            }
            _ => {}
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_1 as *const among,
        40 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_24 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_25 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_26 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_27 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        5 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                8 as ::core::ffi::c_int,
                &raw const s_28 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        6 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_29 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        7 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_30 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        8 => {
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_31 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        9 => {
            let mut ret_7: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_32 as *const symbol,
            );
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
        }
        10 => {
            let mut ret_8: ::core::ffi::c_int = slice_from_s(
                z,
                12 as ::core::ffi::c_int,
                &raw const s_33 as *const symbol,
            );
            if ret_8 < 0 as ::core::ffi::c_int {
                return ret_8;
            }
        }
        11 => {
            let mut ret_9: ::core::ffi::c_int = slice_from_s(
                z,
                10 as ::core::ffi::c_int,
                &raw const s_34 as *const symbol,
            );
            if ret_9 < 0 as ::core::ffi::c_int {
                return ret_9;
            }
        }
        _ => {}
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_3 as *const among, 14 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_2 as *const among,
        31 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_35 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_36 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_5 as *const among, 7 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_4 as *const among, 8 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        4 as ::core::ffi::c_int,
        &raw const s_37 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s3(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if eq_s_b(z, 6 as ::core::ffi::c_int, &raw const s_38 as *const symbol) == 0 {
        current_block = 14818246667978755368;
    } else {
        (*z).bra = (*z).c;
        if (*z).c > (*z).lb {
            current_block = 14818246667978755368;
        } else {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_39 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            current_block = 14807526118172260336;
        }
    }
    match current_block {
        14818246667978755368 => {
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
        }
        _ => {}
    }
    if find_among_b(z, &raw const a_7 as *const among, 7 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_6 as *const among,
        32 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    match among_var {
        1 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_40 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        2 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_41 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s4(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_9 as *const among, 7 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 5 as ::core::ffi::c_int
        || -(2145255424 as ::core::ffi::c_int)
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_8 as *const among, 19 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        2 as ::core::ffi::c_int,
        &raw const s_42 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s5(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_11 as *const among, 11 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_10 as *const among,
        40 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    match among_var {
        1 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_43 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        2 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_44 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s6(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_14 as *const among, 6 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 181 as ::core::ffi::c_int
    {
        current_block = 9580158904786522280;
    } else {
        among_var = find_among_b(
            z,
            &raw const a_12 as *const among,
            7 as ::core::ffi::c_int,
        );
        if among_var == 0 {
            current_block = 9580158904786522280;
        } else if (*z).c > (*z).lb {
            current_block = 9580158904786522280;
        } else {
            match among_var {
                1 => {
                    let mut ret_0: ::core::ffi::c_int = slice_from_s(
                        z,
                        6 as ::core::ffi::c_int,
                        &raw const s_45 as *const symbol,
                    );
                    if ret_0 < 0 as ::core::ffi::c_int {
                        return ret_0;
                    }
                }
                2 => {
                    let mut ret_1: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_46 as *const symbol,
                    );
                    if ret_1 < 0 as ::core::ffi::c_int {
                        return ret_1;
                    }
                }
                _ => {}
            }
            current_block = 7540665535617897051;
        }
    }
    match current_block {
        9580158904786522280 => {
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            if (*z).c - 9 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 186 as ::core::ffi::c_int
                    && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 189 as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            among_var = find_among_b(
                z,
                &raw const a_13 as *const among,
                10 as ::core::ffi::c_int,
            );
            if among_var == 0 {
                return 0 as ::core::ffi::c_int;
            }
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    let mut ret_2: ::core::ffi::c_int = slice_from_s(
                        z,
                        12 as ::core::ffi::c_int,
                        &raw const s_47 as *const symbol,
                    );
                    if ret_2 < 0 as ::core::ffi::c_int {
                        return ret_2;
                    }
                }
                2 => {
                    let mut ret_3: ::core::ffi::c_int = slice_from_s(
                        z,
                        8 as ::core::ffi::c_int,
                        &raw const s_48 as *const symbol,
                    );
                    if ret_3 < 0 as ::core::ffi::c_int {
                        return ret_3;
                    }
                }
                3 => {
                    let mut ret_4: ::core::ffi::c_int = slice_from_s(
                        z,
                        10 as ::core::ffi::c_int,
                        &raw const s_49 as *const symbol,
                    );
                    if ret_4 < 0 as ::core::ffi::c_int {
                        return ret_4;
                    }
                }
                4 => {
                    let mut ret_5: ::core::ffi::c_int = slice_from_s(
                        z,
                        6 as ::core::ffi::c_int,
                        &raw const s_50 as *const symbol,
                    );
                    if ret_5 < 0 as ::core::ffi::c_int {
                        return ret_5;
                    }
                }
                5 => {
                    let mut ret_6: ::core::ffi::c_int = slice_from_s(
                        z,
                        12 as ::core::ffi::c_int,
                        &raw const s_51 as *const symbol,
                    );
                    if ret_6 < 0 as ::core::ffi::c_int {
                        return ret_6;
                    }
                }
                6 => {
                    let mut ret_7: ::core::ffi::c_int = slice_from_s(
                        z,
                        10 as ::core::ffi::c_int,
                        &raw const s_52 as *const symbol,
                    );
                    if ret_7 < 0 as ::core::ffi::c_int {
                        return ret_7;
                    }
                }
                7 => {
                    let mut ret_8: ::core::ffi::c_int = slice_from_s(
                        z,
                        6 as ::core::ffi::c_int,
                        &raw const s_53 as *const symbol,
                    );
                    if ret_8 < 0 as ::core::ffi::c_int {
                        return ret_8;
                    }
                }
                8 => {
                    let mut ret_9: ::core::ffi::c_int = slice_from_s(
                        z,
                        16 as ::core::ffi::c_int,
                        &raw const s_54 as *const symbol,
                    );
                    if ret_9 < 0 as ::core::ffi::c_int {
                        return ret_9;
                    }
                }
                9 => {
                    let mut ret_10: ::core::ffi::c_int = slice_from_s(
                        z,
                        12 as ::core::ffi::c_int,
                        &raw const s_55 as *const symbol,
                    );
                    if ret_10 < 0 as ::core::ffi::c_int {
                        return ret_10;
                    }
                }
                10 => {
                    let mut ret_11: ::core::ffi::c_int = slice_from_s(
                        z,
                        10 as ::core::ffi::c_int,
                        &raw const s_56 as *const symbol,
                    );
                    if ret_11 < 0 as ::core::ffi::c_int {
                        return ret_11;
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s7(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 9 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 177 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 185 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_16 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 131 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 135 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_15 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        8 as ::core::ffi::c_int,
        &raw const s_57 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s8(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_18 as *const among, 8 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_17 as *const among,
        46 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        current_block = 18099066637544488618;
    } else if (*z).c > (*z).lb {
        current_block = 18099066637544488618;
    } else {
        match among_var {
            1 => {
                let mut ret_0: ::core::ffi::c_int = slice_from_s(
                    z,
                    4 as ::core::ffi::c_int,
                    &raw const s_58 as *const symbol,
                );
                if ret_0 < 0 as ::core::ffi::c_int {
                    return ret_0;
                }
            }
            2 => {
                let mut ret_1: ::core::ffi::c_int = slice_from_s(
                    z,
                    6 as ::core::ffi::c_int,
                    &raw const s_59 as *const symbol,
                );
                if ret_1 < 0 as ::core::ffi::c_int {
                    return ret_1;
                }
            }
            _ => {}
        }
        current_block = 13815691605197449446;
    }
    match current_block {
        18099066637544488618 => {
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if eq_s_b(z, 6 as ::core::ffi::c_int, &raw const s_60 as *const symbol) == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_61 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s9(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    (*z).ket = (*z).c;
    if (*z).c - 7 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 5 as ::core::ffi::c_int
        || -(1610481664 as ::core::ffi::c_int)
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_21 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_19 as *const among, 4 as ::core::ffi::c_int) == 0 {
        current_block = 18440589067675258433;
    } else if (*z).c > (*z).lb {
        current_block = 18440589067675258433;
    } else {
        let mut ret_0: ::core::ffi::c_int = slice_from_s(
            z,
            4 as ::core::ffi::c_int,
            &raw const s_62 as *const symbol,
        );
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
        current_block = 5584426685615227130;
    }
    match current_block {
        18440589067675258433 => {
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 181 as ::core::ffi::c_int
                    && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 189 as ::core::ffi::c_int
            {
                return 0 as ::core::ffi::c_int;
            }
            if find_among_b(z, &raw const a_20 as *const among, 2 as ::core::ffi::c_int)
                == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_63 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_s10(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_23 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_22 as *const among, 7 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        6 as ::core::ffi::c_int,
        &raw const s_64 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_2a(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 131 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 189 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_24 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if find_among_b(z, &raw const a_25 as *const among, 10 as ::core::ffi::c_int) == 0 {
        (*z).c = (*z).l - m1;
    } else {
        return 0 as ::core::ffi::c_int
    }
    let mut ret_0: ::core::ffi::c_int = 0;
    let mut saved_c: ::core::ffi::c_int = (*z).c;
    ret_0 = insert_s(
        z,
        (*z).c,
        (*z).c,
        4 as ::core::ffi::c_int,
        &raw const s_65 as *const symbol,
    );
    (*z).c = saved_c;
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_2b(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 131 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 189 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_26 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 128 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 187 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_27 as *const among, 8 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        4 as ::core::ffi::c_int,
        &raw const s_66 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_2c(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 9 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 131 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 189 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_28 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_29 as *const among, 15 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        6 as ::core::ffi::c_int,
        &raw const s_67 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_2d(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 5 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 131 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 189 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_30 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_31 as *const among, 8 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        2 as ::core::ffi::c_int,
        &raw const s_68 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_3(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_32 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if in_grouping_b_U(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        945 as ::core::ffi::c_int,
        969 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        2 as ::core::ffi::c_int,
        &raw const s_69 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_4(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_33 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if in_grouping_b_U(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        945 as ::core::ffi::c_int,
        969 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        (*z).c = (*z).l - m1;
        (*z).ket = (*z).c;
    } else {
        let mut ret_0: ::core::ffi::c_int = slice_from_s(
            z,
            4 as ::core::ffi::c_int,
            &raw const s_70 as *const symbol,
        );
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
    }
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_34 as *const among, 36 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_1: ::core::ffi::c_int = slice_from_s(
        z,
        4 as ::core::ffi::c_int,
        &raw const s_71 as *const symbol,
    );
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5a(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !(eq_s_b(z, 10 as ::core::ffi::c_int, &raw const s_72 as *const symbol) == 0) {
        (*z).bra = (*z).c;
        if !((*z).c > (*z).lb) {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                8 as ::core::ffi::c_int,
                &raw const s_73 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
    }
    (*z).c = (*z).l - m1;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !((*z).c - 9 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 181 as ::core::ffi::c_int)
    {
        if !(find_among_b(z, &raw const a_35 as *const among, 5 as ::core::ffi::c_int)
            == 0)
        {
            (*z).bra = (*z).c;
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
        }
    }
    (*z).c = (*z).l - m2;
    (*z).ket = (*z).c;
    if eq_s_b(z, 6 as ::core::ffi::c_int, &raw const s_74 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_1: ::core::ffi::c_int = slice_del(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_36 as *const among, 12 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_2: ::core::ffi::c_int = slice_from_s(
        z,
        4 as ::core::ffi::c_int,
        &raw const s_75 as *const symbol,
    );
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5b(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !((*z).c - 9 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 181 as ::core::ffi::c_int)
    {
        if !(find_among_b(z, &raw const a_38 as *const among, 11 as ::core::ffi::c_int)
            == 0)
        {
            (*z).bra = (*z).c;
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if !((*z).c - 3 as ::core::ffi::c_int <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 129 as ::core::ffi::c_int
                    && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 131 as ::core::ffi::c_int)
            {
                if !(find_among_b(
                    z,
                    &raw const a_37 as *const among,
                    2 as ::core::ffi::c_int,
                ) == 0)
                {
                    if !((*z).c > (*z).lb) {
                        let mut ret_0: ::core::ffi::c_int = slice_from_s(
                            z,
                            8 as ::core::ffi::c_int,
                            &raw const s_76 as *const symbol,
                        );
                        if ret_0 < 0 as ::core::ffi::c_int {
                            return ret_0;
                        }
                    }
                }
            }
        }
    }
    (*z).c = (*z).l - m1;
    (*z).ket = (*z).c;
    if eq_s_b(z, 6 as ::core::ffi::c_int, &raw const s_77 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_1: ::core::ffi::c_int = slice_del(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if in_grouping_b_U(
        z,
        &raw const g_v2 as *const ::core::ffi::c_uchar,
        945 as ::core::ffi::c_int,
        969 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        (*z).c = (*z).l - m2;
        (*z).ket = (*z).c;
    } else {
        let mut ret_2: ::core::ffi::c_int = slice_from_s(
            z,
            4 as ::core::ffi::c_int,
            &raw const s_78 as *const symbol,
        );
        if ret_2 < 0 as ::core::ffi::c_int {
            return ret_2;
        }
    }
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_39 as *const among, 95 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_3: ::core::ffi::c_int = slice_from_s(
        z,
        4 as ::core::ffi::c_int,
        &raw const s_79 as *const symbol,
    );
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5c(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !((*z).c - 9 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 181 as ::core::ffi::c_int)
    {
        if !(find_among_b(z, &raw const a_40 as *const among, 1 as ::core::ffi::c_int)
            == 0)
        {
            (*z).bra = (*z).c;
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
        }
    }
    (*z).c = (*z).l - m1;
    (*z).ket = (*z).c;
    if eq_s_b(z, 6 as ::core::ffi::c_int, &raw const s_80 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if in_grouping_b_U(
        z,
        &raw const g_v2 as *const ::core::ffi::c_uchar,
        945 as ::core::ffi::c_int,
        969 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        (*z).c = (*z).l - m2;
        (*z).ket = (*z).c;
        (*z).bra = (*z).c;
        if find_among_b(z, &raw const a_41 as *const among, 31 as ::core::ffi::c_int)
            == 0
        {
            (*z).c = (*z).l - m2;
            (*z).ket = (*z).c;
        } else {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_82 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
    } else {
        let mut ret_1: ::core::ffi::c_int = slice_from_s(
            z,
            4 as ::core::ffi::c_int,
            &raw const s_81 as *const symbol,
        );
        if ret_1 < 0 as ::core::ffi::c_int {
            return ret_1;
        }
    }
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_42 as *const among, 25 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_3: ::core::ffi::c_int = slice_from_s(
        z,
        4 as ::core::ffi::c_int,
        &raw const s_83 as *const symbol,
    );
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5d(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    (*z).ket = (*z).c;
    if (*z).c - 9 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 131 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_43 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if eq_s_b(z, 6 as ::core::ffi::c_int, &raw const s_84 as *const symbol) == 0 {
        current_block = 10482660236517565885;
    } else if (*z).c > (*z).lb {
        current_block = 10482660236517565885;
    } else {
        let mut ret_0: ::core::ffi::c_int = slice_from_s(
            z,
            6 as ::core::ffi::c_int,
            &raw const s_85 as *const symbol,
        );
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
        current_block = 948564772133043597;
    }
    match current_block {
        10482660236517565885 => {
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if eq_s_b(z, 6 as ::core::ffi::c_int, &raw const s_86 as *const symbol) == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_87 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5e(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 11 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 181 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_44 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if eq_s_b(z, 4 as ::core::ffi::c_int, &raw const s_88 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        10 as ::core::ffi::c_int,
        &raw const s_89 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5f(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !(eq_s_b(z, 10 as ::core::ffi::c_int, &raw const s_90 as *const symbol) == 0) {
        (*z).bra = (*z).c;
        let mut ret: ::core::ffi::c_int = slice_del(z);
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
        *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
        (*z).ket = (*z).c;
        (*z).bra = (*z).c;
        if !((*z).c - 1 as ::core::ffi::c_int <= (*z).lb
            || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 128 as ::core::ffi::c_int
                && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 134 as ::core::ffi::c_int)
        {
            if !(find_among_b(
                z,
                &raw const a_45 as *const among,
                6 as ::core::ffi::c_int,
            ) == 0)
            {
                if !((*z).c > (*z).lb) {
                    let mut ret_0: ::core::ffi::c_int = slice_from_s(
                        z,
                        8 as ::core::ffi::c_int,
                        &raw const s_91 as *const symbol,
                    );
                    if ret_0 < 0 as ::core::ffi::c_int {
                        return ret_0;
                    }
                }
            }
        }
    }
    (*z).c = (*z).l - m1;
    (*z).ket = (*z).c;
    if eq_s_b(z, 8 as ::core::ffi::c_int, &raw const s_92 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_1: ::core::ffi::c_int = slice_del(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_46 as *const among, 9 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_2: ::core::ffi::c_int = slice_from_s(
        z,
        8 as ::core::ffi::c_int,
        &raw const s_93 as *const symbol,
    );
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5g(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !(find_among_b(z, &raw const a_47 as *const among, 3 as ::core::ffi::c_int) == 0)
    {
        (*z).bra = (*z).c;
        let mut ret: ::core::ffi::c_int = slice_del(z);
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
        *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    }
    (*z).c = (*z).l - m1;
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_50 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_48 as *const among, 6 as ::core::ffi::c_int) == 0 {
        (*z).c = (*z).l - m2;
        (*z).ket = (*z).c;
        (*z).bra = (*z).c;
        if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
            || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 184 as ::core::ffi::c_int
        {
            return 0 as ::core::ffi::c_int;
        }
        if find_among_b(z, &raw const a_49 as *const among, 5 as ::core::ffi::c_int) == 0
        {
            return 0 as ::core::ffi::c_int;
        }
        if (*z).c > (*z).lb {
            return 0 as ::core::ffi::c_int;
        }
        let mut ret_2: ::core::ffi::c_int = slice_from_s(
            z,
            4 as ::core::ffi::c_int,
            &raw const s_95 as *const symbol,
        );
        if ret_2 < 0 as ::core::ffi::c_int {
            return ret_2;
        }
    } else {
        let mut ret_1: ::core::ffi::c_int = slice_from_s(
            z,
            4 as ::core::ffi::c_int,
            &raw const s_94 as *const symbol,
        );
        if ret_1 < 0 as ::core::ffi::c_int {
            return ret_1;
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5h(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_53 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_51 as *const among, 12 as ::core::ffi::c_int) == 0 {
        (*z).c = (*z).l - m1;
        (*z).ket = (*z).c;
        (*z).bra = (*z).c;
        if find_among_b(z, &raw const a_52 as *const among, 25 as ::core::ffi::c_int)
            == 0
        {
            return 0 as ::core::ffi::c_int;
        }
        if (*z).c > (*z).lb {
            return 0 as ::core::ffi::c_int;
        }
        let mut ret_1: ::core::ffi::c_int = slice_from_s(
            z,
            6 as ::core::ffi::c_int,
            &raw const s_97 as *const symbol,
        );
        if ret_1 < 0 as ::core::ffi::c_int {
            return ret_1;
        }
    } else {
        let mut ret_0: ::core::ffi::c_int = slice_from_s(
            z,
            6 as ::core::ffi::c_int,
            &raw const s_96 as *const symbol,
        );
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5i(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_56 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if eq_s_b(z, 8 as ::core::ffi::c_int, &raw const s_98 as *const symbol) == 0 {
        (*z).c = (*z).l - m1;
        let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
        (*z).ket = (*z).c;
        (*z).bra = (*z).c;
        among_var = find_among_b(
            z,
            &raw const a_54 as *const among,
            12 as ::core::ffi::c_int,
        );
        if among_var == 0 {
            (*z).c = (*z).l - m2;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if find_among_b(z, &raw const a_55 as *const among, 44 as ::core::ffi::c_int)
                == 0
            {
                return 0 as ::core::ffi::c_int;
            }
            if (*z).c > (*z).lb {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_101 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        } else {
            match among_var {
                1 => {
                    let mut ret_1: ::core::ffi::c_int = slice_from_s(
                        z,
                        4 as ::core::ffi::c_int,
                        &raw const s_100 as *const symbol,
                    );
                    if ret_1 < 0 as ::core::ffi::c_int {
                        return ret_1;
                    }
                }
                _ => {}
            }
        }
    } else {
        let mut ret_0: ::core::ffi::c_int = slice_from_s(
            z,
            4 as ::core::ffi::c_int,
            &raw const s_99 as *const symbol,
        );
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5j(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_57 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 189 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_58 as *const among, 6 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        4 as ::core::ffi::c_int,
        &raw const s_102 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5k(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 181 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_59 as *const among, 1 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_60 as *const among, 10 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        6 as ::core::ffi::c_int,
        &raw const s_103 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5l(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 181 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_61 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_62 as *const among, 6 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        6 as ::core::ffi::c_int,
        &raw const s_104 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_5m(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 181 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_63 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, &raw const a_64 as *const among, 7 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = slice_from_s(
        z,
        6 as ::core::ffi::c_int,
        &raw const s_105 as *const symbol,
    );
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_6(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    if !(find_among_b(z, &raw const a_65 as *const among, 3 as ::core::ffi::c_int) == 0)
    {
        (*z).bra = (*z).c;
        let mut ret: ::core::ffi::c_int = slice_from_s(
            z,
            4 as ::core::ffi::c_int,
            &raw const s_106 as *const symbol,
        );
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
    }
    (*z).c = (*z).l - m1;
    if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_66 as *const among, 84 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret_0: ::core::ffi::c_int = slice_del(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_step_7(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 129 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 132 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_67 as *const among, 8 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn greek_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret: ::core::ffi::c_int = r_tolower(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = (*z).l - m1;
    let mut ret_0: ::core::ffi::c_int = r_has_min_length(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_step_1(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_step_s1(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m3;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_step_s2(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = (*z).l - m4;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_4: ::core::ffi::c_int = r_step_s3(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    (*z).c = (*z).l - m5;
    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_5: ::core::ffi::c_int = r_step_s4(z);
    if ret_5 < 0 as ::core::ffi::c_int {
        return ret_5;
    }
    (*z).c = (*z).l - m6;
    let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_6: ::core::ffi::c_int = r_step_s5(z);
    if ret_6 < 0 as ::core::ffi::c_int {
        return ret_6;
    }
    (*z).c = (*z).l - m7;
    let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_7: ::core::ffi::c_int = r_step_s6(z);
    if ret_7 < 0 as ::core::ffi::c_int {
        return ret_7;
    }
    (*z).c = (*z).l - m8;
    let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_8: ::core::ffi::c_int = r_step_s7(z);
    if ret_8 < 0 as ::core::ffi::c_int {
        return ret_8;
    }
    (*z).c = (*z).l - m9;
    let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_9: ::core::ffi::c_int = r_step_s8(z);
    if ret_9 < 0 as ::core::ffi::c_int {
        return ret_9;
    }
    (*z).c = (*z).l - m10;
    let mut m11: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_10: ::core::ffi::c_int = r_step_s9(z);
    if ret_10 < 0 as ::core::ffi::c_int {
        return ret_10;
    }
    (*z).c = (*z).l - m11;
    let mut m12: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_11: ::core::ffi::c_int = r_step_s10(z);
    if ret_11 < 0 as ::core::ffi::c_int {
        return ret_11;
    }
    (*z).c = (*z).l - m12;
    let mut m13: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_12: ::core::ffi::c_int = r_step_2a(z);
    if ret_12 < 0 as ::core::ffi::c_int {
        return ret_12;
    }
    (*z).c = (*z).l - m13;
    let mut m14: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_13: ::core::ffi::c_int = r_step_2b(z);
    if ret_13 < 0 as ::core::ffi::c_int {
        return ret_13;
    }
    (*z).c = (*z).l - m14;
    let mut m15: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_14: ::core::ffi::c_int = r_step_2c(z);
    if ret_14 < 0 as ::core::ffi::c_int {
        return ret_14;
    }
    (*z).c = (*z).l - m15;
    let mut m16: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_15: ::core::ffi::c_int = r_step_2d(z);
    if ret_15 < 0 as ::core::ffi::c_int {
        return ret_15;
    }
    (*z).c = (*z).l - m16;
    let mut m17: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_16: ::core::ffi::c_int = r_step_3(z);
    if ret_16 < 0 as ::core::ffi::c_int {
        return ret_16;
    }
    (*z).c = (*z).l - m17;
    let mut m18: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_17: ::core::ffi::c_int = r_step_4(z);
    if ret_17 < 0 as ::core::ffi::c_int {
        return ret_17;
    }
    (*z).c = (*z).l - m18;
    let mut m19: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_18: ::core::ffi::c_int = r_step_5a(z);
    if ret_18 < 0 as ::core::ffi::c_int {
        return ret_18;
    }
    (*z).c = (*z).l - m19;
    let mut m20: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_19: ::core::ffi::c_int = r_step_5b(z);
    if ret_19 < 0 as ::core::ffi::c_int {
        return ret_19;
    }
    (*z).c = (*z).l - m20;
    let mut m21: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_20: ::core::ffi::c_int = r_step_5c(z);
    if ret_20 < 0 as ::core::ffi::c_int {
        return ret_20;
    }
    (*z).c = (*z).l - m21;
    let mut m22: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_21: ::core::ffi::c_int = r_step_5d(z);
    if ret_21 < 0 as ::core::ffi::c_int {
        return ret_21;
    }
    (*z).c = (*z).l - m22;
    let mut m23: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_22: ::core::ffi::c_int = r_step_5e(z);
    if ret_22 < 0 as ::core::ffi::c_int {
        return ret_22;
    }
    (*z).c = (*z).l - m23;
    let mut m24: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_23: ::core::ffi::c_int = r_step_5f(z);
    if ret_23 < 0 as ::core::ffi::c_int {
        return ret_23;
    }
    (*z).c = (*z).l - m24;
    let mut m25: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_24: ::core::ffi::c_int = r_step_5g(z);
    if ret_24 < 0 as ::core::ffi::c_int {
        return ret_24;
    }
    (*z).c = (*z).l - m25;
    let mut m26: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_25: ::core::ffi::c_int = r_step_5h(z);
    if ret_25 < 0 as ::core::ffi::c_int {
        return ret_25;
    }
    (*z).c = (*z).l - m26;
    let mut m27: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_26: ::core::ffi::c_int = r_step_5j(z);
    if ret_26 < 0 as ::core::ffi::c_int {
        return ret_26;
    }
    (*z).c = (*z).l - m27;
    let mut m28: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_27: ::core::ffi::c_int = r_step_5i(z);
    if ret_27 < 0 as ::core::ffi::c_int {
        return ret_27;
    }
    (*z).c = (*z).l - m28;
    let mut m29: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_28: ::core::ffi::c_int = r_step_5k(z);
    if ret_28 < 0 as ::core::ffi::c_int {
        return ret_28;
    }
    (*z).c = (*z).l - m29;
    let mut m30: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_29: ::core::ffi::c_int = r_step_5l(z);
    if ret_29 < 0 as ::core::ffi::c_int {
        return ret_29;
    }
    (*z).c = (*z).l - m30;
    let mut m31: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_30: ::core::ffi::c_int = r_step_5m(z);
    if ret_30 < 0 as ::core::ffi::c_int {
        return ret_30;
    }
    (*z).c = (*z).l - m31;
    let mut m32: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_31: ::core::ffi::c_int = r_step_6(z);
    if ret_31 < 0 as ::core::ffi::c_int {
        return ret_31;
    }
    (*z).c = (*z).l - m32;
    let mut m33: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_32: ::core::ffi::c_int = r_step_7(z);
    if ret_32 < 0 as ::core::ffi::c_int {
        return ret_32;
    }
    (*z).c = (*z).l - m33;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn greek_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 1 as ::core::ffi::c_int);
}
pub unsafe fn greek_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
