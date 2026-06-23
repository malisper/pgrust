use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 1] = ['m' as i32 as symbol];
static mut s_0_1: [symbol; 1] = ['n' as i32 as symbol];
static mut s_0_2: [symbol; 3] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_0_3: [symbol; 3] = [
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_0_4: [symbol; 3] = [
    'm' as i32 as symbol,
    'u' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_0_5: [symbol; 3] = [
    'n' as i32 as symbol,
    'u' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_0_6: [symbol; 4] = [
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut s_0_7: [symbol; 4] = [
    'n' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut s_0_8: [symbol; 4] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut s_0_9: [symbol; 4] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut a_0: [among; 10] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
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
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 4] = [
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_1: [symbol; 5] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 2] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 2] = ['n' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_1: [symbol; 2] = ['n' as i32 as symbol, 'u' as i32 as symbol];
static mut s_2_2: [symbol; 3] = [
    'n' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_2_3: [symbol; 3] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 4] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 2] = ['i' as i32 as symbol, 'n' as i32 as symbol];
static mut s_3_1: [symbol; 2] = ['u' as i32 as symbol, 'n' as i32 as symbol];
static mut s_3_2: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_3_3: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
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
    ]
};
static mut s_4_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_4_1: [symbol; 1] = ['e' as i32 as symbol];
static mut a_4: [among; 2] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = ['n' as i32 as symbol, 'a' as i32 as symbol];
static mut s_5_1: [symbol; 2] = ['n' as i32 as symbol, 'e' as i32 as symbol];
static mut a_5: [among; 2] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 2] = ['d' as i32 as symbol, 'a' as i32 as symbol];
static mut s_6_1: [symbol; 2] = ['t' as i32 as symbol, 'a' as i32 as symbol];
static mut s_6_2: [symbol; 2] = ['d' as i32 as symbol, 'e' as i32 as symbol];
static mut s_6_3: [symbol; 2] = ['t' as i32 as symbol, 'e' as i32 as symbol];
static mut a_6: [among; 4] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 3] = [
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_7_1: [symbol; 3] = [
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut a_7: [among; 2] = unsafe {
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
    ]
};
static mut s_8_0: [symbol; 3] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_1: [symbol; 3] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_2: [symbol; 3] = [
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_8_3: [symbol; 3] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut a_8: [among; 4] = unsafe {
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
    ]
};
static mut s_9_0: [symbol; 4] = [
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_9_1: [symbol; 4] = [
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut a_9: [among; 2] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_9_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_9_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_10_0: [symbol; 2] = ['l' as i32 as symbol, 'a' as i32 as symbol];
static mut s_10_1: [symbol; 2] = ['l' as i32 as symbol, 'e' as i32 as symbol];
static mut a_10: [among; 2] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_11_0: [symbol; 2] = ['c' as i32 as symbol, 'a' as i32 as symbol];
static mut s_11_1: [symbol; 2] = ['c' as i32 as symbol, 'e' as i32 as symbol];
static mut a_11: [among; 2] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_12_0: [symbol; 2] = ['i' as i32 as symbol, 'm' as i32 as symbol];
static mut s_12_1: [symbol; 2] = ['u' as i32 as symbol, 'm' as i32 as symbol];
static mut s_12_2: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_12_3: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut a_12: [among; 4] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_12_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
    ]
};
static mut s_13_0: [symbol; 3] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_13_1: [symbol; 3] = [
    's' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_13_2: [symbol; 4] = [
    's' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_13_3: [symbol; 4] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut a_13: [among; 4] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_13_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_13_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_13_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_13_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_14_0: [symbol; 2] = ['i' as i32 as symbol, 'z' as i32 as symbol];
static mut s_14_1: [symbol; 2] = ['u' as i32 as symbol, 'z' as i32 as symbol];
static mut s_14_2: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut s_14_3: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut a_14: [among; 4] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_14_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_14_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_15_0: [symbol; 5] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_15_1: [symbol; 5] = [
    's' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'u' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_15_2: [symbol; 7] = [
    's' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut s_15_3: [symbol; 7] = [
    's' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut a_15: [among; 4] = unsafe {
    [
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_15_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_15_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_15_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_15_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_16_0: [symbol; 3] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_16_1: [symbol; 3] = [
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut a_16: [among; 2] = unsafe {
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
    ]
};
static mut s_17_0: [symbol; 3] = [
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_17_1: [symbol; 3] = [
    'n' as i32 as symbol,
    'u' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_17_2: [symbol; 4] = [
    'n' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut s_17_3: [symbol; 4] = [
    'n' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'z' as i32 as symbol,
];
static mut a_17: [among; 4] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_17_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_17_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_18_0: [symbol; 3] = [
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_18_1: [symbol; 3] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_18_2: [symbol; 3] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_18_3: [symbol; 3] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_18_4: [symbol; 4] = [
    'd' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
];
static mut s_18_5: [symbol; 4] = [
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
];
static mut s_18_6: [symbol; 4] = [
    'd' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
];
static mut s_18_7: [symbol; 4] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'r' as i32 as symbol,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_19_0: [symbol; 7] = [
    'c' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_19_1: [symbol; 6] = [
    'c' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut a_19: [among; 2] = unsafe {
    [
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_19_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_19_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_20_0: [symbol; 2] = ['d' as i32 as symbol, 'i' as i32 as symbol];
static mut s_20_1: [symbol; 2] = ['t' as i32 as symbol, 'i' as i32 as symbol];
static mut s_20_2: [symbol; 3] = [
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_20_3: [symbol; 3] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_20_4: [symbol; 3] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_20_5: [symbol; 3] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_20_6: [symbol; 4] = [
    'd' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_20_7: [symbol; 4] = [
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_20_8: [symbol; 4] = [
    'd' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_20_9: [symbol; 4] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_20_10: [symbol; 3] = [
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_20_11: [symbol; 3] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_20_12: [symbol; 3] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_20_13: [symbol; 3] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_20_14: [symbol; 4] = [
    'd' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_20_15: [symbol; 4] = [
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_20_16: [symbol; 4] = [
    'd' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_20_17: [symbol; 4] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_20_18: [symbol; 3] = [
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_20_19: [symbol; 3] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_20_20: [symbol; 3] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_20_21: [symbol; 3] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_20_22: [symbol; 4] = [
    'd' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_20_23: [symbol; 4] = [
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_20_24: [symbol; 4] = [
    'd' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_20_25: [symbol; 4] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_20_26: [symbol; 2] = ['d' as i32 as symbol, 'u' as i32 as symbol];
static mut s_20_27: [symbol; 2] = ['t' as i32 as symbol, 'u' as i32 as symbol];
static mut s_20_28: [symbol; 3] = [
    'd' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_20_29: [symbol; 3] = [
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_20_30: [symbol; 3] = [
    'd' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_20_31: [symbol; 3] = [
    't' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut a_20: [among; 32] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_20_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_20_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_20_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_20_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_20_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_20_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_21_0: [symbol; 2] = ['s' as i32 as symbol, 'a' as i32 as symbol];
static mut s_21_1: [symbol; 2] = ['s' as i32 as symbol, 'e' as i32 as symbol];
static mut s_21_2: [symbol; 3] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_21_3: [symbol; 3] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_21_4: [symbol; 3] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_21_5: [symbol; 3] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_21_6: [symbol; 3] = [
    's' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_21_7: [symbol; 3] = [
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut a_21: [among; 8] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_21_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_21_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_21_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_22_0: [symbol; 4] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_22_1: [symbol; 4] = [
    'm' as i32 as symbol,
    'u' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_22_2: [symbol; 5] = [
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_22_3: [symbol; 5] = [
    'm' as i32 as symbol,
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut a_22: [among; 4] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_22_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_22_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_22_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_22_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_23_0: [symbol; 1] = ['b' as i32 as symbol];
static mut s_23_1: [symbol; 1] = ['c' as i32 as symbol];
static mut s_23_2: [symbol; 1] = ['d' as i32 as symbol];
static mut s_23_3: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut a_23: [among; 4] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_23_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_23_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_23_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_23_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_vowel: [::core::ffi::c_uchar; 27] = [
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
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    32 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_U: [::core::ffi::c_uchar; 26] = [
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    8 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_vowel1: [::core::ffi::c_uchar; 27] = [
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    64 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
];
static mut g_vowel2: [::core::ffi::c_uchar; 19] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
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
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    130 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_vowel3: [::core::ffi::c_uchar; 27] = [
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
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_vowel4: [::core::ffi::c_uchar; 1] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_vowel5: [::core::ffi::c_uchar; 1] = [
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_vowel6: [::core::ffi::c_uchar; 1] = [
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_1: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_2: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_3: [symbol; 2] = ['k' as i32 as symbol, 'i' as i32 as symbol];
static mut s_4: [symbol; 3] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_5: [symbol; 1] = ['p' as i32 as symbol];
static mut s_6: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_7: [symbol; 1] = ['t' as i32 as symbol];
static mut s_8: [symbol; 1] = ['k' as i32 as symbol];
static mut s_9: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_10: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_11: [symbol; 1] = ['i' as i32 as symbol];
static mut s_12: [symbol; 1] = ['u' as i32 as symbol];
static mut s_13: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_14: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_15: [symbol; 2] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_16: [symbol; 2] = ['a' as i32 as symbol, 'd' as i32 as symbol];
static mut s_17: [symbol; 3] = [
    's' as i32 as symbol,
    'o' as i32 as symbol,
    'y' as i32 as symbol,
];
unsafe fn r_check_vowel_harmony(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut m_test1: ::core::ffi::c_int = (*z).l - (*z).c;
    if out_grouping_b_U(
        z,
        &raw const g_vowel as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        305 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    ) < 0 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'a' as i32
    {
        current_block = 10711865538819913068;
    } else {
        (*z).c -= 1;
        if out_grouping_b_U(
            z,
            &raw const g_vowel1 as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            305 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        ) < 0 as ::core::ffi::c_int
        {
            current_block = 10711865538819913068;
        } else {
            current_block = 1655704410784438243;
        }
    }
    match current_block {
        10711865538819913068 => {
            (*z).c = (*z).l - m2;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'e' as i32
            {
                current_block = 4180862646881766146;
            } else {
                (*z).c -= 1;
                if out_grouping_b_U(
                    z,
                    &raw const g_vowel2 as *const ::core::ffi::c_uchar,
                    101 as ::core::ffi::c_int,
                    252 as ::core::ffi::c_int,
                    1 as ::core::ffi::c_int,
                ) < 0 as ::core::ffi::c_int
                {
                    current_block = 4180862646881766146;
                } else {
                    current_block = 1655704410784438243;
                }
            }
            match current_block {
                1655704410784438243 => {}
                _ => {
                    (*z).c = (*z).l - m2;
                    if eq_s_b(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_0 as *const symbol,
                    ) == 0
                    {
                        current_block = 8193144676602674933;
                    } else if out_grouping_b_U(
                        z,
                        &raw const g_vowel3 as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        305 as ::core::ffi::c_int,
                        1 as ::core::ffi::c_int,
                    ) < 0 as ::core::ffi::c_int
                    {
                        current_block = 8193144676602674933;
                    } else {
                        current_block = 1655704410784438243;
                    }
                    match current_block {
                        1655704410784438243 => {}
                        _ => {
                            (*z).c = (*z).l - m2;
                            if (*z).c <= (*z).lb
                                || *(*z)
                                    .p
                                    .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                    as ::core::ffi::c_int != 'i' as i32
                            {
                                current_block = 3285653399674207684;
                            } else {
                                (*z).c -= 1;
                                if out_grouping_b_U(
                                    z,
                                    &raw const g_vowel4 as *const ::core::ffi::c_uchar,
                                    101 as ::core::ffi::c_int,
                                    105 as ::core::ffi::c_int,
                                    1 as ::core::ffi::c_int,
                                ) < 0 as ::core::ffi::c_int
                                {
                                    current_block = 3285653399674207684;
                                } else {
                                    current_block = 1655704410784438243;
                                }
                            }
                            match current_block {
                                1655704410784438243 => {}
                                _ => {
                                    (*z).c = (*z).l - m2;
                                    if (*z).c <= (*z).lb
                                        || *(*z)
                                            .p
                                            .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                            as ::core::ffi::c_int != 'o' as i32
                                    {
                                        current_block = 12246471050194814546;
                                    } else {
                                        (*z).c -= 1;
                                        if out_grouping_b_U(
                                            z,
                                            &raw const g_vowel5 as *const ::core::ffi::c_uchar,
                                            111 as ::core::ffi::c_int,
                                            117 as ::core::ffi::c_int,
                                            1 as ::core::ffi::c_int,
                                        ) < 0 as ::core::ffi::c_int
                                        {
                                            current_block = 12246471050194814546;
                                        } else {
                                            current_block = 1655704410784438243;
                                        }
                                    }
                                    match current_block {
                                        1655704410784438243 => {}
                                        _ => {
                                            (*z).c = (*z).l - m2;
                                            if eq_s_b(
                                                z,
                                                2 as ::core::ffi::c_int,
                                                &raw const s_1 as *const symbol,
                                            ) == 0
                                            {
                                                current_block = 7767067499259563090;
                                            } else if out_grouping_b_U(
                                                z,
                                                &raw const g_vowel6 as *const ::core::ffi::c_uchar,
                                                246 as ::core::ffi::c_int,
                                                252 as ::core::ffi::c_int,
                                                1 as ::core::ffi::c_int,
                                            ) < 0 as ::core::ffi::c_int
                                            {
                                                current_block = 7767067499259563090;
                                            } else {
                                                current_block = 1655704410784438243;
                                            }
                                            match current_block {
                                                1655704410784438243 => {}
                                                _ => {
                                                    (*z).c = (*z).l - m2;
                                                    if (*z).c <= (*z).lb
                                                        || *(*z)
                                                            .p
                                                            .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                                            as ::core::ffi::c_int != 'u' as i32
                                                    {
                                                        current_block = 2438352751361416361;
                                                    } else {
                                                        (*z).c -= 1;
                                                        if out_grouping_b_U(
                                                            z,
                                                            &raw const g_vowel5 as *const ::core::ffi::c_uchar,
                                                            111 as ::core::ffi::c_int,
                                                            117 as ::core::ffi::c_int,
                                                            1 as ::core::ffi::c_int,
                                                        ) < 0 as ::core::ffi::c_int
                                                        {
                                                            current_block = 2438352751361416361;
                                                        } else {
                                                            current_block = 1655704410784438243;
                                                        }
                                                    }
                                                    match current_block {
                                                        1655704410784438243 => {}
                                                        _ => {
                                                            (*z).c = (*z).l - m2;
                                                            if eq_s_b(
                                                                z,
                                                                2 as ::core::ffi::c_int,
                                                                &raw const s_2 as *const symbol,
                                                            ) == 0
                                                            {
                                                                return 0 as ::core::ffi::c_int;
                                                            }
                                                            if out_grouping_b_U(
                                                                z,
                                                                &raw const g_vowel6 as *const ::core::ffi::c_uchar,
                                                                246 as ::core::ffi::c_int,
                                                                252 as ::core::ffi::c_int,
                                                                1 as ::core::ffi::c_int,
                                                            ) < 0 as ::core::ffi::c_int
                                                            {
                                                                return 0 as ::core::ffi::c_int;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    (*z).c = (*z).l - m_test1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_suffix_with_optional_n_consonant(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'n' as i32
    {
        current_block = 5136985158474231060;
    } else {
        (*z).c -= 1;
        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
        if in_grouping_b_U(
            z,
            &raw const g_vowel as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            305 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0
        {
            current_block = 5136985158474231060;
        } else {
            (*z).c = (*z).l - m_test2;
            current_block = 11729455853407927995;
        }
    }
    match current_block {
        5136985158474231060 => {
            (*z).c = (*z).l - m1;
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m_test4: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'n' as i32
            {
                (*z).c = (*z).l - m3;
            } else {
                (*z).c -= 1;
                (*z).c = (*z).l - m_test4;
                return 0 as ::core::ffi::c_int;
            }
            let mut m_test5: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret: ::core::ffi::c_int = skip_b_utf8(
                (*z).p,
                (*z).c,
                (*z).lb,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = ret;
            if in_grouping_b_U(
                z,
                &raw const g_vowel as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                305 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = (*z).l - m_test5;
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_suffix_with_optional_s_consonant(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 's' as i32
    {
        current_block = 10766308341712289000;
    } else {
        (*z).c -= 1;
        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
        if in_grouping_b_U(
            z,
            &raw const g_vowel as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            305 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0
        {
            current_block = 10766308341712289000;
        } else {
            (*z).c = (*z).l - m_test2;
            current_block = 16660934473602739489;
        }
    }
    match current_block {
        10766308341712289000 => {
            (*z).c = (*z).l - m1;
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m_test4: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 's' as i32
            {
                (*z).c = (*z).l - m3;
            } else {
                (*z).c -= 1;
                (*z).c = (*z).l - m_test4;
                return 0 as ::core::ffi::c_int;
            }
            let mut m_test5: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret: ::core::ffi::c_int = skip_b_utf8(
                (*z).p,
                (*z).c,
                (*z).lb,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = ret;
            if in_grouping_b_U(
                z,
                &raw const g_vowel as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                305 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = (*z).l - m_test5;
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_suffix_with_optional_y_consonant(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'y' as i32
    {
        current_block = 10555405479196348808;
    } else {
        (*z).c -= 1;
        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
        if in_grouping_b_U(
            z,
            &raw const g_vowel as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            305 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0
        {
            current_block = 10555405479196348808;
        } else {
            (*z).c = (*z).l - m_test2;
            current_block = 2568476814325912383;
        }
    }
    match current_block {
        10555405479196348808 => {
            (*z).c = (*z).l - m1;
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m_test4: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'y' as i32
            {
                (*z).c = (*z).l - m3;
            } else {
                (*z).c -= 1;
                (*z).c = (*z).l - m_test4;
                return 0 as ::core::ffi::c_int;
            }
            let mut m_test5: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret: ::core::ffi::c_int = skip_b_utf8(
                (*z).p,
                (*z).c,
                (*z).lb,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = ret;
            if in_grouping_b_U(
                z,
                &raw const g_vowel as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                305 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = (*z).l - m_test5;
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_suffix_with_optional_U_vowel(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if in_grouping_b_U(
        z,
        &raw const g_U as *const ::core::ffi::c_uchar,
        105 as ::core::ffi::c_int,
        305 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 9623383628966751235;
    } else {
        let mut m_test2: ::core::ffi::c_int = (*z).l - (*z).c;
        if out_grouping_b_U(
            z,
            &raw const g_vowel as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            305 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0
        {
            current_block = 9623383628966751235;
        } else {
            (*z).c = (*z).l - m_test2;
            current_block = 16376211907481692128;
        }
    }
    match current_block {
        9623383628966751235 => {
            (*z).c = (*z).l - m1;
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m_test4: ::core::ffi::c_int = (*z).l - (*z).c;
            if in_grouping_b_U(
                z,
                &raw const g_U as *const ::core::ffi::c_uchar,
                105 as ::core::ffi::c_int,
                305 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                (*z).c = (*z).l - m3;
            } else {
                (*z).c = (*z).l - m_test4;
                return 0 as ::core::ffi::c_int;
            }
            let mut m_test5: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret: ::core::ffi::c_int = skip_b_utf8(
                (*z).p,
                (*z).c,
                (*z).lb,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = ret;
            if out_grouping_b_U(
                z,
                &raw const g_vowel as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                305 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                return 0 as ::core::ffi::c_int;
            }
            (*z).c = (*z).l - m_test5;
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_possessives(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 67133440 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_0 as *const among, 10 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = r_mark_suffix_with_optional_U_vowel(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_sU(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if in_grouping_b_U(
        z,
        &raw const g_U as *const ::core::ffi::c_uchar,
        105 as ::core::ffi::c_int,
        305 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_s_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_lArI(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 105 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 177 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_1 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_yU(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if in_grouping_b_U(
        z,
        &raw const g_U as *const ::core::ffi::c_uchar,
        105 as ::core::ffi::c_int,
        305 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_y_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_nU(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if find_among_b(z, &raw const a_2 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_nUn(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 110 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_3 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_n_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_yA(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_4 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_y_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_nA(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_5 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_DA(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_6 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_ndA(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_7 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_DAn(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 110 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_8 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_ndAn(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 110 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_9 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_ylA(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_10 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_y_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_ki(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_3 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_ncA(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_11 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_n_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_yUm(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 109 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_12 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_y_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_sUn(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 110 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_13 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_yUz(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 122 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_14 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_y_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_sUnUz(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if (*z).c - 4 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 122 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_15 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_lAr(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 114 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_16 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_nUz(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 122 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_17 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_DUr(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 114 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_18 as *const among, 8 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_cAsInA(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if (*z).c - 5 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_19 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_yDU(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if find_among_b(z, &raw const a_20 as *const among, 32 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_y_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_ysA(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 26658 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_21 as *const among, 8 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = r_mark_suffix_with_optional_y_consonant(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_ymUs_(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_check_vowel_harmony(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 159 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_22 as *const among, 4 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret_0: ::core::ffi::c_int = r_mark_suffix_with_optional_y_consonant(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_mark_yken(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_4 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = r_mark_suffix_with_optional_y_consonant(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_stem_nominal_verb_suffixes(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    (*z).ket = (*z).c;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret: ::core::ffi::c_int = r_mark_ymUs_(z);
    if ret == 0 as ::core::ffi::c_int {
        (*z).c = (*z).l - m2;
        let mut ret_0: ::core::ffi::c_int = r_mark_yDU(z);
        if ret_0 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m2;
            let mut ret_1: ::core::ffi::c_int = r_mark_ysA(z);
            if ret_1 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m2;
                let mut ret_2: ::core::ffi::c_int = r_mark_yken(z);
                if ret_2 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m1;
                    let mut ret_3: ::core::ffi::c_int = r_mark_cAsInA(z);
                    if ret_3 == 0 as ::core::ffi::c_int {
                        current_block = 6612018757316515492;
                    } else {
                        if ret_3 < 0 as ::core::ffi::c_int {
                            return ret_3;
                        }
                        let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
                        let mut ret_4: ::core::ffi::c_int = r_mark_sUnUz(z);
                        if ret_4 == 0 as ::core::ffi::c_int {
                            (*z).c = (*z).l - m3;
                            let mut ret_5: ::core::ffi::c_int = r_mark_lAr(z);
                            if ret_5 == 0 as ::core::ffi::c_int {
                                (*z).c = (*z).l - m3;
                                let mut ret_6: ::core::ffi::c_int = r_mark_yUm(z);
                                if ret_6 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m3;
                                    let mut ret_7: ::core::ffi::c_int = r_mark_sUn(z);
                                    if ret_7 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m3;
                                        let mut ret_8: ::core::ffi::c_int = r_mark_yUz(z);
                                        if ret_8 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m3;
                                        } else if ret_8 < 0 as ::core::ffi::c_int {
                                            return ret_8
                                        }
                                    } else if ret_7 < 0 as ::core::ffi::c_int {
                                        return ret_7
                                    }
                                } else if ret_6 < 0 as ::core::ffi::c_int {
                                    return ret_6
                                }
                            } else if ret_5 < 0 as ::core::ffi::c_int {
                                return ret_5
                            }
                        } else if ret_4 < 0 as ::core::ffi::c_int {
                            return ret_4
                        }
                        let mut ret_9: ::core::ffi::c_int = r_mark_ymUs_(z);
                        if ret_9 == 0 as ::core::ffi::c_int {
                            current_block = 6612018757316515492;
                        } else {
                            if ret_9 < 0 as ::core::ffi::c_int {
                                return ret_9;
                            }
                            current_block = 8277373342746637944;
                        }
                    }
                    match current_block {
                        8277373342746637944 => {}
                        _ => {
                            (*z).c = (*z).l - m1;
                            let mut ret_10: ::core::ffi::c_int = r_mark_lAr(z);
                            if ret_10 == 0 as ::core::ffi::c_int {
                                (*z).c = (*z).l - m1;
                                let mut ret_16: ::core::ffi::c_int = r_mark_nUz(z);
                                if ret_16 == 0 as ::core::ffi::c_int {
                                    current_block = 1532251304746091844;
                                } else {
                                    if ret_16 < 0 as ::core::ffi::c_int {
                                        return ret_16;
                                    }
                                    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                                    let mut ret_17: ::core::ffi::c_int = r_mark_yDU(z);
                                    if ret_17 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m6;
                                        let mut ret_18: ::core::ffi::c_int = r_mark_ysA(z);
                                        if ret_18 == 0 as ::core::ffi::c_int {
                                            current_block = 1532251304746091844;
                                        } else {
                                            if ret_18 < 0 as ::core::ffi::c_int {
                                                return ret_18;
                                            }
                                            current_block = 8277373342746637944;
                                        }
                                    } else {
                                        if ret_17 < 0 as ::core::ffi::c_int {
                                            return ret_17;
                                        }
                                        current_block = 8277373342746637944;
                                    }
                                }
                                match current_block {
                                    8277373342746637944 => {}
                                    _ => {
                                        (*z).c = (*z).l - m1;
                                        let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                                        let mut ret_19: ::core::ffi::c_int = r_mark_sUnUz(z);
                                        if ret_19 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m7;
                                            let mut ret_20: ::core::ffi::c_int = r_mark_yUz(z);
                                            if ret_20 == 0 as ::core::ffi::c_int {
                                                (*z).c = (*z).l - m7;
                                                let mut ret_21: ::core::ffi::c_int = r_mark_sUn(z);
                                                if ret_21 == 0 as ::core::ffi::c_int {
                                                    (*z).c = (*z).l - m7;
                                                    let mut ret_22: ::core::ffi::c_int = r_mark_yUm(z);
                                                    if ret_22 == 0 as ::core::ffi::c_int {
                                                        (*z).c = (*z).l - m1;
                                                        let mut ret_25: ::core::ffi::c_int = r_mark_DUr(z);
                                                        if ret_25 <= 0 as ::core::ffi::c_int {
                                                            return ret_25;
                                                        }
                                                        (*z).bra = (*z).c;
                                                        let mut ret_26: ::core::ffi::c_int = slice_del(z);
                                                        if ret_26 < 0 as ::core::ffi::c_int {
                                                            return ret_26;
                                                        }
                                                        let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
                                                        (*z).ket = (*z).c;
                                                        let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
                                                        let mut ret_27: ::core::ffi::c_int = r_mark_sUnUz(z);
                                                        if ret_27 == 0 as ::core::ffi::c_int {
                                                            (*z).c = (*z).l - m10;
                                                            let mut ret_28: ::core::ffi::c_int = r_mark_lAr(z);
                                                            if ret_28 == 0 as ::core::ffi::c_int {
                                                                (*z).c = (*z).l - m10;
                                                                let mut ret_29: ::core::ffi::c_int = r_mark_yUm(z);
                                                                if ret_29 == 0 as ::core::ffi::c_int {
                                                                    (*z).c = (*z).l - m10;
                                                                    let mut ret_30: ::core::ffi::c_int = r_mark_sUn(z);
                                                                    if ret_30 == 0 as ::core::ffi::c_int {
                                                                        (*z).c = (*z).l - m10;
                                                                        let mut ret_31: ::core::ffi::c_int = r_mark_yUz(z);
                                                                        if ret_31 == 0 as ::core::ffi::c_int {
                                                                            (*z).c = (*z).l - m10;
                                                                        } else if ret_31 < 0 as ::core::ffi::c_int {
                                                                            return ret_31
                                                                        }
                                                                    } else if ret_30 < 0 as ::core::ffi::c_int {
                                                                        return ret_30
                                                                    }
                                                                } else if ret_29 < 0 as ::core::ffi::c_int {
                                                                    return ret_29
                                                                }
                                                            } else if ret_28 < 0 as ::core::ffi::c_int {
                                                                return ret_28
                                                            }
                                                        } else if ret_27 < 0 as ::core::ffi::c_int {
                                                            return ret_27
                                                        }
                                                        let mut ret_32: ::core::ffi::c_int = r_mark_ymUs_(z);
                                                        if ret_32 == 0 as ::core::ffi::c_int {
                                                            (*z).c = (*z).l - m9;
                                                        } else if ret_32 < 0 as ::core::ffi::c_int {
                                                            return ret_32
                                                        }
                                                        current_block = 8277373342746637944;
                                                    } else {
                                                        if ret_22 < 0 as ::core::ffi::c_int {
                                                            return ret_22;
                                                        }
                                                        current_block = 4868870142928098146;
                                                    }
                                                } else {
                                                    if ret_21 < 0 as ::core::ffi::c_int {
                                                        return ret_21;
                                                    }
                                                    current_block = 4868870142928098146;
                                                }
                                            } else {
                                                if ret_20 < 0 as ::core::ffi::c_int {
                                                    return ret_20;
                                                }
                                                current_block = 4868870142928098146;
                                            }
                                        } else {
                                            if ret_19 < 0 as ::core::ffi::c_int {
                                                return ret_19;
                                            }
                                            current_block = 4868870142928098146;
                                        }
                                        match current_block {
                                            8277373342746637944 => {}
                                            _ => {
                                                (*z).bra = (*z).c;
                                                let mut ret_23: ::core::ffi::c_int = slice_del(z);
                                                if ret_23 < 0 as ::core::ffi::c_int {
                                                    return ret_23;
                                                }
                                                let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                                                (*z).ket = (*z).c;
                                                let mut ret_24: ::core::ffi::c_int = r_mark_ymUs_(z);
                                                if ret_24 == 0 as ::core::ffi::c_int {
                                                    (*z).c = (*z).l - m8;
                                                } else if ret_24 < 0 as ::core::ffi::c_int {
                                                    return ret_24
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                if ret_10 < 0 as ::core::ffi::c_int {
                                    return ret_10;
                                }
                                (*z).bra = (*z).c;
                                let mut ret_11: ::core::ffi::c_int = slice_del(z);
                                if ret_11 < 0 as ::core::ffi::c_int {
                                    return ret_11;
                                }
                                let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
                                (*z).ket = (*z).c;
                                let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
                                let mut ret_12: ::core::ffi::c_int = r_mark_DUr(z);
                                if ret_12 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m5;
                                    let mut ret_13: ::core::ffi::c_int = r_mark_yDU(z);
                                    if ret_13 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m5;
                                        let mut ret_14: ::core::ffi::c_int = r_mark_ysA(z);
                                        if ret_14 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m5;
                                            let mut ret_15: ::core::ffi::c_int = r_mark_ymUs_(z);
                                            if ret_15 == 0 as ::core::ffi::c_int {
                                                (*z).c = (*z).l - m4;
                                            } else if ret_15 < 0 as ::core::ffi::c_int {
                                                return ret_15
                                            }
                                        } else if ret_14 < 0 as ::core::ffi::c_int {
                                            return ret_14
                                        }
                                    } else if ret_13 < 0 as ::core::ffi::c_int {
                                        return ret_13
                                    }
                                } else if ret_12 < 0 as ::core::ffi::c_int {
                                    return ret_12
                                }
                                *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0
                                    as ::core::ffi::c_int;
                            }
                        }
                    }
                } else if ret_2 < 0 as ::core::ffi::c_int {
                    return ret_2
                }
            } else if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1
            }
        } else if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0
        }
    } else if ret < 0 as ::core::ffi::c_int {
        return ret
    }
    (*z).bra = (*z).c;
    let mut ret_33: ::core::ffi::c_int = slice_del(z);
    if ret_33 < 0 as ::core::ffi::c_int {
        return ret_33;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_stem_suffix_chain_before_ki(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    (*z).ket = (*z).c;
    let mut ret: ::core::ffi::c_int = r_mark_ki(z);
    if ret <= 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_mark_DA(z);
    if ret_0 == 0 as ::core::ffi::c_int {
        (*z).c = (*z).l - m1;
        let mut ret_10: ::core::ffi::c_int = r_mark_nUn(z);
        if ret_10 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m1;
            let mut ret_21: ::core::ffi::c_int = r_mark_ndA(z);
            if ret_21 <= 0 as ::core::ffi::c_int {
                return ret_21;
            }
            let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_22: ::core::ffi::c_int = r_mark_lArI(z);
            if ret_22 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m10;
                let mut ret_24: ::core::ffi::c_int = r_mark_sU(z);
                if ret_24 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m10;
                    let mut ret_29: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                        z,
                    );
                    if ret_29 <= 0 as ::core::ffi::c_int {
                        return ret_29;
                    }
                } else {
                    if ret_24 < 0 as ::core::ffi::c_int {
                        return ret_24;
                    }
                    (*z).bra = (*z).c;
                    let mut ret_25: ::core::ffi::c_int = slice_del(z);
                    if ret_25 < 0 as ::core::ffi::c_int {
                        return ret_25;
                    }
                    let mut m11: ::core::ffi::c_int = (*z).l - (*z).c;
                    (*z).ket = (*z).c;
                    let mut ret_26: ::core::ffi::c_int = r_mark_lAr(z);
                    if ret_26 == 0 as ::core::ffi::c_int {
                        (*z).c = (*z).l - m11;
                    } else {
                        if ret_26 < 0 as ::core::ffi::c_int {
                            return ret_26;
                        }
                        (*z).bra = (*z).c;
                        let mut ret_27: ::core::ffi::c_int = slice_del(z);
                        if ret_27 < 0 as ::core::ffi::c_int {
                            return ret_27;
                        }
                        let mut ret_28: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                            z,
                        );
                        if ret_28 == 0 as ::core::ffi::c_int {
                            (*z).c = (*z).l - m11;
                        } else if ret_28 < 0 as ::core::ffi::c_int {
                            return ret_28
                        }
                    }
                }
            } else {
                if ret_22 < 0 as ::core::ffi::c_int {
                    return ret_22;
                }
                (*z).bra = (*z).c;
                let mut ret_23: ::core::ffi::c_int = slice_del(z);
                if ret_23 < 0 as ::core::ffi::c_int {
                    return ret_23;
                }
            }
        } else {
            if ret_10 < 0 as ::core::ffi::c_int {
                return ret_10;
            }
            (*z).bra = (*z).c;
            let mut ret_11: ::core::ffi::c_int = slice_del(z);
            if ret_11 < 0 as ::core::ffi::c_int {
                return ret_11;
            }
            let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_12: ::core::ffi::c_int = r_mark_lArI(z);
            if ret_12 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m7;
                (*z).ket = (*z).c;
                let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_14: ::core::ffi::c_int = r_mark_possessives(z);
                if ret_14 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m8;
                    let mut ret_15: ::core::ffi::c_int = r_mark_sU(z);
                    if ret_15 == 0 as ::core::ffi::c_int {
                        (*z).c = (*z).l - m7;
                        let mut ret_20: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                            z,
                        );
                        if ret_20 == 0 as ::core::ffi::c_int {
                            (*z).c = (*z).l - m6;
                        } else if ret_20 < 0 as ::core::ffi::c_int {
                            return ret_20
                        }
                        current_block = 9098473726789065310;
                    } else {
                        if ret_15 < 0 as ::core::ffi::c_int {
                            return ret_15;
                        }
                        current_block = 13532541658342065255;
                    }
                } else {
                    if ret_14 < 0 as ::core::ffi::c_int {
                        return ret_14;
                    }
                    current_block = 13532541658342065255;
                }
                match current_block {
                    9098473726789065310 => {}
                    _ => {
                        (*z).bra = (*z).c;
                        let mut ret_16: ::core::ffi::c_int = slice_del(z);
                        if ret_16 < 0 as ::core::ffi::c_int {
                            return ret_16;
                        }
                        let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
                        (*z).ket = (*z).c;
                        let mut ret_17: ::core::ffi::c_int = r_mark_lAr(z);
                        if ret_17 == 0 as ::core::ffi::c_int {
                            (*z).c = (*z).l - m9;
                        } else {
                            if ret_17 < 0 as ::core::ffi::c_int {
                                return ret_17;
                            }
                            (*z).bra = (*z).c;
                            let mut ret_18: ::core::ffi::c_int = slice_del(z);
                            if ret_18 < 0 as ::core::ffi::c_int {
                                return ret_18;
                            }
                            let mut ret_19: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                z,
                            );
                            if ret_19 == 0 as ::core::ffi::c_int {
                                (*z).c = (*z).l - m9;
                            } else if ret_19 < 0 as ::core::ffi::c_int {
                                return ret_19
                            }
                        }
                    }
                }
            } else {
                if ret_12 < 0 as ::core::ffi::c_int {
                    return ret_12;
                }
                (*z).bra = (*z).c;
                let mut ret_13: ::core::ffi::c_int = slice_del(z);
                if ret_13 < 0 as ::core::ffi::c_int {
                    return ret_13;
                }
            }
        }
    } else {
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
        (*z).bra = (*z).c;
        let mut ret_1: ::core::ffi::c_int = slice_del(z);
        if ret_1 < 0 as ::core::ffi::c_int {
            return ret_1;
        }
        let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
        (*z).ket = (*z).c;
        let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_2: ::core::ffi::c_int = r_mark_lAr(z);
        if ret_2 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m3;
            let mut ret_5: ::core::ffi::c_int = r_mark_possessives(z);
            if ret_5 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m2;
            } else {
                if ret_5 < 0 as ::core::ffi::c_int {
                    return ret_5;
                }
                (*z).bra = (*z).c;
                let mut ret_6: ::core::ffi::c_int = slice_del(z);
                if ret_6 < 0 as ::core::ffi::c_int {
                    return ret_6;
                }
                let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
                (*z).ket = (*z).c;
                let mut ret_7: ::core::ffi::c_int = r_mark_lAr(z);
                if ret_7 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m5;
                } else {
                    if ret_7 < 0 as ::core::ffi::c_int {
                        return ret_7;
                    }
                    (*z).bra = (*z).c;
                    let mut ret_8: ::core::ffi::c_int = slice_del(z);
                    if ret_8 < 0 as ::core::ffi::c_int {
                        return ret_8;
                    }
                    let mut ret_9: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(z);
                    if ret_9 == 0 as ::core::ffi::c_int {
                        (*z).c = (*z).l - m5;
                    } else if ret_9 < 0 as ::core::ffi::c_int {
                        return ret_9
                    }
                }
            }
        } else {
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
            (*z).bra = (*z).c;
            let mut ret_3: ::core::ffi::c_int = slice_del(z);
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_4: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(z);
            if ret_4 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m4;
            } else if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4
            }
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_stem_noun_suffixes(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    (*z).ket = (*z).c;
    let mut ret: ::core::ffi::c_int = r_mark_lAr(z);
    if ret == 0 as ::core::ffi::c_int {
        (*z).c = (*z).l - m1;
        (*z).ket = (*z).c;
        let mut ret_2: ::core::ffi::c_int = r_mark_ncA(z);
        if ret_2 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_15: ::core::ffi::c_int = r_mark_ndA(z);
            if ret_15 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m7;
                let mut ret_16: ::core::ffi::c_int = r_mark_nA(z);
                if ret_16 == 0 as ::core::ffi::c_int {
                    current_block = 3282659514390828718;
                } else {
                    if ret_16 < 0 as ::core::ffi::c_int {
                        return ret_16;
                    }
                    current_block = 178030534879405462;
                }
            } else {
                if ret_15 < 0 as ::core::ffi::c_int {
                    return ret_15;
                }
                current_block = 178030534879405462;
            }
            match current_block {
                178030534879405462 => {
                    let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                    let mut ret_17: ::core::ffi::c_int = r_mark_lArI(z);
                    if ret_17 == 0 as ::core::ffi::c_int {
                        (*z).c = (*z).l - m8;
                        let mut ret_19: ::core::ffi::c_int = r_mark_sU(z);
                        if ret_19 == 0 as ::core::ffi::c_int {
                            (*z).c = (*z).l - m8;
                            let mut ret_24: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                z,
                            );
                            if ret_24 == 0 as ::core::ffi::c_int {
                                current_block = 3282659514390828718;
                            } else {
                                if ret_24 < 0 as ::core::ffi::c_int {
                                    return ret_24;
                                }
                                current_block = 18226426736697592660;
                            }
                        } else {
                            if ret_19 < 0 as ::core::ffi::c_int {
                                return ret_19;
                            }
                            (*z).bra = (*z).c;
                            let mut ret_20: ::core::ffi::c_int = slice_del(z);
                            if ret_20 < 0 as ::core::ffi::c_int {
                                return ret_20;
                            }
                            let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
                            (*z).ket = (*z).c;
                            let mut ret_21: ::core::ffi::c_int = r_mark_lAr(z);
                            if ret_21 == 0 as ::core::ffi::c_int {
                                (*z).c = (*z).l - m9;
                            } else {
                                if ret_21 < 0 as ::core::ffi::c_int {
                                    return ret_21;
                                }
                                (*z).bra = (*z).c;
                                let mut ret_22: ::core::ffi::c_int = slice_del(z);
                                if ret_22 < 0 as ::core::ffi::c_int {
                                    return ret_22;
                                }
                                let mut ret_23: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                    z,
                                );
                                if ret_23 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m9;
                                } else if ret_23 < 0 as ::core::ffi::c_int {
                                    return ret_23
                                }
                            }
                            current_block = 18226426736697592660;
                        }
                    } else {
                        if ret_17 < 0 as ::core::ffi::c_int {
                            return ret_17;
                        }
                        (*z).bra = (*z).c;
                        let mut ret_18: ::core::ffi::c_int = slice_del(z);
                        if ret_18 < 0 as ::core::ffi::c_int {
                            return ret_18;
                        }
                        current_block = 18226426736697592660;
                    }
                }
                _ => {}
            }
            match current_block {
                18226426736697592660 => {}
                _ => {
                    (*z).c = (*z).l - m1;
                    (*z).ket = (*z).c;
                    let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
                    let mut ret_25: ::core::ffi::c_int = r_mark_ndAn(z);
                    if ret_25 == 0 as ::core::ffi::c_int {
                        (*z).c = (*z).l - m10;
                        let mut ret_26: ::core::ffi::c_int = r_mark_nU(z);
                        if ret_26 == 0 as ::core::ffi::c_int {
                            current_block = 14185688529008047702;
                        } else {
                            if ret_26 < 0 as ::core::ffi::c_int {
                                return ret_26;
                            }
                            current_block = 13174377073168946860;
                        }
                    } else {
                        if ret_25 < 0 as ::core::ffi::c_int {
                            return ret_25;
                        }
                        current_block = 13174377073168946860;
                    }
                    match current_block {
                        13174377073168946860 => {
                            let mut m11: ::core::ffi::c_int = (*z).l - (*z).c;
                            let mut ret_27: ::core::ffi::c_int = r_mark_sU(z);
                            if ret_27 == 0 as ::core::ffi::c_int {
                                (*z).c = (*z).l - m11;
                                let mut ret_32: ::core::ffi::c_int = r_mark_lArI(z);
                                if ret_32 == 0 as ::core::ffi::c_int {
                                    current_block = 14185688529008047702;
                                } else {
                                    if ret_32 < 0 as ::core::ffi::c_int {
                                        return ret_32;
                                    }
                                    current_block = 18226426736697592660;
                                }
                            } else {
                                if ret_27 < 0 as ::core::ffi::c_int {
                                    return ret_27;
                                }
                                (*z).bra = (*z).c;
                                let mut ret_28: ::core::ffi::c_int = slice_del(z);
                                if ret_28 < 0 as ::core::ffi::c_int {
                                    return ret_28;
                                }
                                let mut m12: ::core::ffi::c_int = (*z).l - (*z).c;
                                (*z).ket = (*z).c;
                                let mut ret_29: ::core::ffi::c_int = r_mark_lAr(z);
                                if ret_29 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m12;
                                } else {
                                    if ret_29 < 0 as ::core::ffi::c_int {
                                        return ret_29;
                                    }
                                    (*z).bra = (*z).c;
                                    let mut ret_30: ::core::ffi::c_int = slice_del(z);
                                    if ret_30 < 0 as ::core::ffi::c_int {
                                        return ret_30;
                                    }
                                    let mut ret_31: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                        z,
                                    );
                                    if ret_31 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m12;
                                    } else if ret_31 < 0 as ::core::ffi::c_int {
                                        return ret_31
                                    }
                                }
                                current_block = 18226426736697592660;
                            }
                        }
                        _ => {}
                    }
                    match current_block {
                        18226426736697592660 => {}
                        _ => {
                            (*z).c = (*z).l - m1;
                            (*z).ket = (*z).c;
                            let mut ret_33: ::core::ffi::c_int = r_mark_DAn(z);
                            if ret_33 == 0 as ::core::ffi::c_int {
                                (*z).c = (*z).l - m1;
                                (*z).ket = (*z).c;
                                let mut m17: ::core::ffi::c_int = (*z).l - (*z).c;
                                let mut ret_44: ::core::ffi::c_int = r_mark_nUn(z);
                                if ret_44 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m17;
                                    let mut ret_45: ::core::ffi::c_int = r_mark_ylA(z);
                                    if ret_45 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m1;
                                        (*z).ket = (*z).c;
                                        let mut ret_57: ::core::ffi::c_int = r_mark_lArI(z);
                                        if ret_57 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m1;
                                            let mut ret_59: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                                z,
                                            );
                                            if ret_59 == 0 as ::core::ffi::c_int {
                                                (*z).c = (*z).l - m1;
                                                (*z).ket = (*z).c;
                                                let mut m22: ::core::ffi::c_int = (*z).l - (*z).c;
                                                let mut ret_60: ::core::ffi::c_int = r_mark_DA(z);
                                                if ret_60 == 0 as ::core::ffi::c_int {
                                                    (*z).c = (*z).l - m22;
                                                    let mut ret_61: ::core::ffi::c_int = r_mark_yU(z);
                                                    if ret_61 == 0 as ::core::ffi::c_int {
                                                        (*z).c = (*z).l - m22;
                                                        let mut ret_62: ::core::ffi::c_int = r_mark_yA(z);
                                                        if ret_62 == 0 as ::core::ffi::c_int {
                                                            (*z).c = (*z).l - m1;
                                                            (*z).ket = (*z).c;
                                                            let mut m26: ::core::ffi::c_int = (*z).l - (*z).c;
                                                            let mut ret_70: ::core::ffi::c_int = r_mark_possessives(z);
                                                            if ret_70 == 0 as ::core::ffi::c_int {
                                                                (*z).c = (*z).l - m26;
                                                                let mut ret_71: ::core::ffi::c_int = r_mark_sU(z);
                                                                if ret_71 <= 0 as ::core::ffi::c_int {
                                                                    return ret_71;
                                                                }
                                                            } else if ret_70 < 0 as ::core::ffi::c_int {
                                                                return ret_70
                                                            }
                                                            (*z).bra = (*z).c;
                                                            let mut ret_72: ::core::ffi::c_int = slice_del(z);
                                                            if ret_72 < 0 as ::core::ffi::c_int {
                                                                return ret_72;
                                                            }
                                                            let mut m27: ::core::ffi::c_int = (*z).l - (*z).c;
                                                            (*z).ket = (*z).c;
                                                            let mut ret_73: ::core::ffi::c_int = r_mark_lAr(z);
                                                            if ret_73 == 0 as ::core::ffi::c_int {
                                                                (*z).c = (*z).l - m27;
                                                            } else {
                                                                if ret_73 < 0 as ::core::ffi::c_int {
                                                                    return ret_73;
                                                                }
                                                                (*z).bra = (*z).c;
                                                                let mut ret_74: ::core::ffi::c_int = slice_del(z);
                                                                if ret_74 < 0 as ::core::ffi::c_int {
                                                                    return ret_74;
                                                                }
                                                                let mut ret_75: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                                                    z,
                                                                );
                                                                if ret_75 == 0 as ::core::ffi::c_int {
                                                                    (*z).c = (*z).l - m27;
                                                                } else if ret_75 < 0 as ::core::ffi::c_int {
                                                                    return ret_75
                                                                }
                                                            }
                                                            current_block = 18226426736697592660;
                                                        } else {
                                                            if ret_62 < 0 as ::core::ffi::c_int {
                                                                return ret_62;
                                                            }
                                                            current_block = 846790826115495087;
                                                        }
                                                    } else {
                                                        if ret_61 < 0 as ::core::ffi::c_int {
                                                            return ret_61;
                                                        }
                                                        current_block = 846790826115495087;
                                                    }
                                                } else {
                                                    if ret_60 < 0 as ::core::ffi::c_int {
                                                        return ret_60;
                                                    }
                                                    current_block = 846790826115495087;
                                                }
                                                match current_block {
                                                    18226426736697592660 => {}
                                                    _ => {
                                                        (*z).bra = (*z).c;
                                                        let mut ret_63: ::core::ffi::c_int = slice_del(z);
                                                        if ret_63 < 0 as ::core::ffi::c_int {
                                                            return ret_63;
                                                        }
                                                        let mut m23: ::core::ffi::c_int = (*z).l - (*z).c;
                                                        (*z).ket = (*z).c;
                                                        let mut m24: ::core::ffi::c_int = (*z).l - (*z).c;
                                                        let mut ret_64: ::core::ffi::c_int = r_mark_possessives(z);
                                                        if ret_64 == 0 as ::core::ffi::c_int {
                                                            (*z).c = (*z).l - m24;
                                                            let mut ret_67: ::core::ffi::c_int = r_mark_lAr(z);
                                                            if ret_67 == 0 as ::core::ffi::c_int {
                                                                (*z).c = (*z).l - m23;
                                                                current_block = 18226426736697592660;
                                                            } else {
                                                                if ret_67 < 0 as ::core::ffi::c_int {
                                                                    return ret_67;
                                                                }
                                                                current_block = 3448696023653767246;
                                                            }
                                                        } else {
                                                            if ret_64 < 0 as ::core::ffi::c_int {
                                                                return ret_64;
                                                            }
                                                            (*z).bra = (*z).c;
                                                            let mut ret_65: ::core::ffi::c_int = slice_del(z);
                                                            if ret_65 < 0 as ::core::ffi::c_int {
                                                                return ret_65;
                                                            }
                                                            let mut m25: ::core::ffi::c_int = (*z).l - (*z).c;
                                                            (*z).ket = (*z).c;
                                                            let mut ret_66: ::core::ffi::c_int = r_mark_lAr(z);
                                                            if ret_66 == 0 as ::core::ffi::c_int {
                                                                (*z).c = (*z).l - m25;
                                                            } else if ret_66 < 0 as ::core::ffi::c_int {
                                                                return ret_66
                                                            }
                                                            current_block = 3448696023653767246;
                                                        }
                                                        match current_block {
                                                            18226426736697592660 => {}
                                                            _ => {
                                                                (*z).bra = (*z).c;
                                                                let mut ret_68: ::core::ffi::c_int = slice_del(z);
                                                                if ret_68 < 0 as ::core::ffi::c_int {
                                                                    return ret_68;
                                                                }
                                                                (*z).ket = (*z).c;
                                                                let mut ret_69: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                                                    z,
                                                                );
                                                                if ret_69 == 0 as ::core::ffi::c_int {
                                                                    (*z).c = (*z).l - m23;
                                                                } else if ret_69 < 0 as ::core::ffi::c_int {
                                                                    return ret_69
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            } else if ret_59 < 0 as ::core::ffi::c_int {
                                                return ret_59
                                            }
                                        } else {
                                            if ret_57 < 0 as ::core::ffi::c_int {
                                                return ret_57;
                                            }
                                            (*z).bra = (*z).c;
                                            let mut ret_58: ::core::ffi::c_int = slice_del(z);
                                            if ret_58 < 0 as ::core::ffi::c_int {
                                                return ret_58;
                                            }
                                        }
                                        current_block = 18226426736697592660;
                                    } else {
                                        if ret_45 < 0 as ::core::ffi::c_int {
                                            return ret_45;
                                        }
                                        current_block = 3621218156565010571;
                                    }
                                } else {
                                    if ret_44 < 0 as ::core::ffi::c_int {
                                        return ret_44;
                                    }
                                    current_block = 3621218156565010571;
                                }
                                match current_block {
                                    18226426736697592660 => {}
                                    _ => {
                                        (*z).bra = (*z).c;
                                        let mut ret_46: ::core::ffi::c_int = slice_del(z);
                                        if ret_46 < 0 as ::core::ffi::c_int {
                                            return ret_46;
                                        }
                                        let mut m18: ::core::ffi::c_int = (*z).l - (*z).c;
                                        let mut m19: ::core::ffi::c_int = (*z).l - (*z).c;
                                        (*z).ket = (*z).c;
                                        let mut ret_47: ::core::ffi::c_int = r_mark_lAr(z);
                                        if ret_47 == 0 as ::core::ffi::c_int {
                                            current_block = 14992349157982313401;
                                        } else {
                                            if ret_47 < 0 as ::core::ffi::c_int {
                                                return ret_47;
                                            }
                                            (*z).bra = (*z).c;
                                            let mut ret_48: ::core::ffi::c_int = slice_del(z);
                                            if ret_48 < 0 as ::core::ffi::c_int {
                                                return ret_48;
                                            }
                                            let mut ret_49: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                                z,
                                            );
                                            if ret_49 == 0 as ::core::ffi::c_int {
                                                current_block = 14992349157982313401;
                                            } else {
                                                if ret_49 < 0 as ::core::ffi::c_int {
                                                    return ret_49;
                                                }
                                                current_block = 18226426736697592660;
                                            }
                                        }
                                        match current_block {
                                            18226426736697592660 => {}
                                            _ => {
                                                (*z).c = (*z).l - m19;
                                                (*z).ket = (*z).c;
                                                let mut m20: ::core::ffi::c_int = (*z).l - (*z).c;
                                                let mut ret_50: ::core::ffi::c_int = r_mark_possessives(z);
                                                if ret_50 == 0 as ::core::ffi::c_int {
                                                    (*z).c = (*z).l - m20;
                                                    let mut ret_51: ::core::ffi::c_int = r_mark_sU(z);
                                                    if ret_51 == 0 as ::core::ffi::c_int {
                                                        (*z).c = (*z).l - m19;
                                                        let mut ret_56: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                                            z,
                                                        );
                                                        if ret_56 == 0 as ::core::ffi::c_int {
                                                            (*z).c = (*z).l - m18;
                                                        } else if ret_56 < 0 as ::core::ffi::c_int {
                                                            return ret_56
                                                        }
                                                        current_block = 18226426736697592660;
                                                    } else {
                                                        if ret_51 < 0 as ::core::ffi::c_int {
                                                            return ret_51;
                                                        }
                                                        current_block = 4790828373660273854;
                                                    }
                                                } else {
                                                    if ret_50 < 0 as ::core::ffi::c_int {
                                                        return ret_50;
                                                    }
                                                    current_block = 4790828373660273854;
                                                }
                                                match current_block {
                                                    18226426736697592660 => {}
                                                    _ => {
                                                        (*z).bra = (*z).c;
                                                        let mut ret_52: ::core::ffi::c_int = slice_del(z);
                                                        if ret_52 < 0 as ::core::ffi::c_int {
                                                            return ret_52;
                                                        }
                                                        let mut m21: ::core::ffi::c_int = (*z).l - (*z).c;
                                                        (*z).ket = (*z).c;
                                                        let mut ret_53: ::core::ffi::c_int = r_mark_lAr(z);
                                                        if ret_53 == 0 as ::core::ffi::c_int {
                                                            (*z).c = (*z).l - m21;
                                                        } else {
                                                            if ret_53 < 0 as ::core::ffi::c_int {
                                                                return ret_53;
                                                            }
                                                            (*z).bra = (*z).c;
                                                            let mut ret_54: ::core::ffi::c_int = slice_del(z);
                                                            if ret_54 < 0 as ::core::ffi::c_int {
                                                                return ret_54;
                                                            }
                                                            let mut ret_55: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                                                z,
                                                            );
                                                            if ret_55 == 0 as ::core::ffi::c_int {
                                                                (*z).c = (*z).l - m21;
                                                            } else if ret_55 < 0 as ::core::ffi::c_int {
                                                                return ret_55
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                if ret_33 < 0 as ::core::ffi::c_int {
                                    return ret_33;
                                }
                                (*z).bra = (*z).c;
                                let mut ret_34: ::core::ffi::c_int = slice_del(z);
                                if ret_34 < 0 as ::core::ffi::c_int {
                                    return ret_34;
                                }
                                let mut m13: ::core::ffi::c_int = (*z).l - (*z).c;
                                (*z).ket = (*z).c;
                                let mut m14: ::core::ffi::c_int = (*z).l - (*z).c;
                                let mut ret_35: ::core::ffi::c_int = r_mark_possessives(z);
                                if ret_35 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m14;
                                    let mut ret_40: ::core::ffi::c_int = r_mark_lAr(z);
                                    if ret_40 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m14;
                                        let mut ret_43: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                            z,
                                        );
                                        if ret_43 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m13;
                                        } else if ret_43 < 0 as ::core::ffi::c_int {
                                            return ret_43
                                        }
                                    } else {
                                        if ret_40 < 0 as ::core::ffi::c_int {
                                            return ret_40;
                                        }
                                        (*z).bra = (*z).c;
                                        let mut ret_41: ::core::ffi::c_int = slice_del(z);
                                        if ret_41 < 0 as ::core::ffi::c_int {
                                            return ret_41;
                                        }
                                        let mut m16: ::core::ffi::c_int = (*z).l - (*z).c;
                                        let mut ret_42: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                            z,
                                        );
                                        if ret_42 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m16;
                                        } else if ret_42 < 0 as ::core::ffi::c_int {
                                            return ret_42
                                        }
                                    }
                                } else {
                                    if ret_35 < 0 as ::core::ffi::c_int {
                                        return ret_35;
                                    }
                                    (*z).bra = (*z).c;
                                    let mut ret_36: ::core::ffi::c_int = slice_del(z);
                                    if ret_36 < 0 as ::core::ffi::c_int {
                                        return ret_36;
                                    }
                                    let mut m15: ::core::ffi::c_int = (*z).l - (*z).c;
                                    (*z).ket = (*z).c;
                                    let mut ret_37: ::core::ffi::c_int = r_mark_lAr(z);
                                    if ret_37 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m15;
                                    } else {
                                        if ret_37 < 0 as ::core::ffi::c_int {
                                            return ret_37;
                                        }
                                        (*z).bra = (*z).c;
                                        let mut ret_38: ::core::ffi::c_int = slice_del(z);
                                        if ret_38 < 0 as ::core::ffi::c_int {
                                            return ret_38;
                                        }
                                        let mut ret_39: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                            z,
                                        );
                                        if ret_39 == 0 as ::core::ffi::c_int {
                                            (*z).c = (*z).l - m15;
                                        } else if ret_39 < 0 as ::core::ffi::c_int {
                                            return ret_39
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
            (*z).bra = (*z).c;
            let mut ret_3: ::core::ffi::c_int = slice_del(z);
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
            let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            let mut ret_4: ::core::ffi::c_int = r_mark_lArI(z);
            if ret_4 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m4;
                (*z).ket = (*z).c;
                let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_6: ::core::ffi::c_int = r_mark_possessives(z);
                if ret_6 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m5;
                    let mut ret_7: ::core::ffi::c_int = r_mark_sU(z);
                    if ret_7 == 0 as ::core::ffi::c_int {
                        (*z).c = (*z).l - m4;
                        (*z).ket = (*z).c;
                        let mut ret_12: ::core::ffi::c_int = r_mark_lAr(z);
                        if ret_12 == 0 as ::core::ffi::c_int {
                            (*z).c = (*z).l - m3;
                        } else {
                            if ret_12 < 0 as ::core::ffi::c_int {
                                return ret_12;
                            }
                            (*z).bra = (*z).c;
                            let mut ret_13: ::core::ffi::c_int = slice_del(z);
                            if ret_13 < 0 as ::core::ffi::c_int {
                                return ret_13;
                            }
                            let mut ret_14: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                z,
                            );
                            if ret_14 == 0 as ::core::ffi::c_int {
                                (*z).c = (*z).l - m3;
                            } else if ret_14 < 0 as ::core::ffi::c_int {
                                return ret_14
                            }
                        }
                        current_block = 18226426736697592660;
                    } else {
                        if ret_7 < 0 as ::core::ffi::c_int {
                            return ret_7;
                        }
                        current_block = 3803426832068870150;
                    }
                } else {
                    if ret_6 < 0 as ::core::ffi::c_int {
                        return ret_6;
                    }
                    current_block = 3803426832068870150;
                }
                match current_block {
                    18226426736697592660 => {}
                    _ => {
                        (*z).bra = (*z).c;
                        let mut ret_8: ::core::ffi::c_int = slice_del(z);
                        if ret_8 < 0 as ::core::ffi::c_int {
                            return ret_8;
                        }
                        let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                        (*z).ket = (*z).c;
                        let mut ret_9: ::core::ffi::c_int = r_mark_lAr(z);
                        if ret_9 == 0 as ::core::ffi::c_int {
                            (*z).c = (*z).l - m6;
                        } else {
                            if ret_9 < 0 as ::core::ffi::c_int {
                                return ret_9;
                            }
                            (*z).bra = (*z).c;
                            let mut ret_10: ::core::ffi::c_int = slice_del(z);
                            if ret_10 < 0 as ::core::ffi::c_int {
                                return ret_10;
                            }
                            let mut ret_11: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(
                                z,
                            );
                            if ret_11 == 0 as ::core::ffi::c_int {
                                (*z).c = (*z).l - m6;
                            } else if ret_11 < 0 as ::core::ffi::c_int {
                                return ret_11
                            }
                        }
                    }
                }
            } else {
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
                (*z).bra = (*z).c;
                let mut ret_5: ::core::ffi::c_int = slice_del(z);
                if ret_5 < 0 as ::core::ffi::c_int {
                    return ret_5;
                }
            }
        }
    } else {
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
        (*z).bra = (*z).c;
        let mut ret_0: ::core::ffi::c_int = slice_del(z);
        if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0;
        }
        let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_1: ::core::ffi::c_int = r_stem_suffix_chain_before_ki(z);
        if ret_1 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m2;
        } else if ret_1 < 0 as ::core::ffi::c_int {
            return ret_1
        }
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_post_process_last_consonants(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_23 as *const among,
        4 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_5 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_6 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_8 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_append_U_to_stems_ending_with_d_or_g(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block: u64;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'd' as i32
    {
        (*z).c = (*z).l - m1;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 'g' as i32
        {
            return 0 as ::core::ffi::c_int;
        }
        (*z).c -= 1;
    } else {
        (*z).c -= 1;
    }
    if out_grouping_b_U(
        z,
        &raw const g_vowel as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        305 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    ) < 0 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 'a' as i32
    {
        (*z).c = (*z).l - m3;
        if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_9 as *const symbol) == 0 {
            (*z).c = (*z).l - m2;
            let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int != 'e' as i32
            {
                (*z).c = (*z).l - m4;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 'i' as i32
                {
                    (*z).c = (*z).l - m2;
                    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
                    if (*z).c <= (*z).lb
                        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                            as ::core::ffi::c_int != 'o' as i32
                    {
                        (*z).c = (*z).l - m5;
                        if (*z).c <= (*z).lb
                            || *(*z)
                                .p
                                .offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                                as ::core::ffi::c_int != 'u' as i32
                        {
                            (*z).c = (*z).l - m2;
                            let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
                            if eq_s_b(
                                z,
                                2 as ::core::ffi::c_int,
                                &raw const s_13 as *const symbol,
                            ) == 0
                            {
                                (*z).c = (*z).l - m6;
                                if eq_s_b(
                                    z,
                                    2 as ::core::ffi::c_int,
                                    &raw const s_14 as *const symbol,
                                ) == 0
                                {
                                    return 0 as ::core::ffi::c_int;
                                }
                            }
                            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                                z,
                                2 as ::core::ffi::c_int,
                                &raw const s_15 as *const symbol,
                            );
                            if ret_2 < 0 as ::core::ffi::c_int {
                                return ret_2;
                            }
                            current_block = 17120961156120710785;
                        } else {
                            (*z).c -= 1;
                            current_block = 7637394576275120116;
                        }
                    } else {
                        (*z).c -= 1;
                        current_block = 7637394576275120116;
                    }
                    match current_block {
                        17120961156120710785 => {}
                        _ => {
                            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                                z,
                                1 as ::core::ffi::c_int,
                                &raw const s_12 as *const symbol,
                            );
                            if ret_1 < 0 as ::core::ffi::c_int {
                                return ret_1;
                            }
                            current_block = 17120961156120710785;
                        }
                    }
                } else {
                    (*z).c -= 1;
                    current_block = 6145058528136866443;
                }
            } else {
                (*z).c -= 1;
                current_block = 6145058528136866443;
            }
            match current_block {
                17120961156120710785 => {}
                _ => {
                    let mut ret_0: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_11 as *const symbol,
                    );
                    if ret_0 < 0 as ::core::ffi::c_int {
                        return ret_0;
                    }
                    current_block = 17120961156120710785;
                }
            }
        } else {
            current_block = 13234927814670729062;
        }
    } else {
        (*z).c -= 1;
        current_block = 13234927814670729062;
    }
    match current_block {
        13234927814670729062 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_10 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_is_reserved_word(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if eq_s_b(z, 2 as ::core::ffi::c_int, &raw const s_16 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if eq_s_b(z, 3 as ::core::ffi::c_int, &raw const s_17 as *const symbol) == 0 {
        (*z).c = (*z).l - m1;
    }
    if (*z).c > (*z).lb {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_proper_noun_suffix(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut current_block_10: u64;
    let mut c1: ::core::ffi::c_int = (*z).c;
    loop {
        let mut c2: ::core::ffi::c_int = (*z).c;
        if (*z).c == (*z).l
            || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int != '\'' as i32
        {
            (*z).c = c2;
            let mut ret: ::core::ffi::c_int = skip_utf8(
                (*z).p,
                (*z).c,
                (*z).l,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                current_block_10 = 2593436951332334351;
                break;
            }
            (*z).c = ret;
        } else {
            (*z).c += 1;
            (*z).c = c2;
            current_block_10 = 8515828400728868193;
            break;
        }
    }
    match current_block_10 {
        8515828400728868193 => {
            (*z).bra = (*z).c;
            (*z).c = (*z).l;
            (*z).ket = (*z).c;
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        _ => {}
    }
    (*z).c = c1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_more_than_one_syllable_word(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut c_test1: ::core::ffi::c_int = (*z).c;
    let mut i: ::core::ffi::c_int = 0;
    i = 2 as ::core::ffi::c_int;
    while i > 0 as ::core::ffi::c_int {
        let mut ret: ::core::ffi::c_int = out_grouping_U(
            z,
            &raw const g_vowel as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            305 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if ret < 0 as ::core::ffi::c_int {
            return 0 as ::core::ffi::c_int;
        }
        (*z).c += ret;
        i -= 1;
    }
    (*z).c = c_test1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_postlude(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret: ::core::ffi::c_int = r_is_reserved_word(z);
    if ret == 0 as ::core::ffi::c_int {
        (*z).c = (*z).l - m1;
    } else {
        if ret < 0 as ::core::ffi::c_int {
            return ret;
        }
        return 0 as ::core::ffi::c_int;
    }
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_append_U_to_stems_ending_with_d_or_g(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_post_process_last_consonants(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m3;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn turkish_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_remove_proper_noun_suffix(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    let mut ret_0: ::core::ffi::c_int = r_more_than_one_syllable_word(z);
    if ret_0 <= 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_stem_nominal_verb_suffixes(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m1;
    if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_stem_noun_suffixes(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m2;
    (*z).c = (*z).lb;
    let mut ret_3: ::core::ffi::c_int = r_postlude(z);
    if ret_3 <= 0 as ::core::ffi::c_int {
        return ret_3;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn turkish_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 1 as ::core::ffi::c_int);
}
pub unsafe fn turkish_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
