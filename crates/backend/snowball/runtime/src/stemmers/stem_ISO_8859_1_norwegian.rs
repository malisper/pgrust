use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 1] = ['a' as i32 as symbol];
static mut s_0_1: [symbol; 1] = ['e' as i32 as symbol];
static mut s_0_2: [symbol; 3] = [
    'e' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_3: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_4: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_5: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_6: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_7: [symbol; 6] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_8: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_9: [symbol; 2] = ['e' as i32 as symbol, 'n' as i32 as symbol];
static mut s_0_10: [symbol; 5] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_11: [symbol; 2] = ['a' as i32 as symbol, 'r' as i32 as symbol];
static mut s_0_12: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_0_13: [symbol; 5] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_0_14: [symbol; 1] = ['s' as i32 as symbol];
static mut s_0_15: [symbol; 2] = ['a' as i32 as symbol, 's' as i32 as symbol];
static mut s_0_16: [symbol; 2] = ['e' as i32 as symbol, 's' as i32 as symbol];
static mut s_0_17: [symbol; 4] = [
    'e' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_18: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_19: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_20: [symbol; 7] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_21: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_22: [symbol; 6] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_23: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_24: [symbol; 3] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_25: [symbol; 2] = ['e' as i32 as symbol, 't' as i32 as symbol];
static mut s_0_26: [symbol; 3] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_0_27: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_0_28: [symbol; 3] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
];
static mut a_0: [among; 29] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_16 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_18 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_19 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_20 as *const symbol,
            substring_i: 19 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_21 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_22 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_23 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_24 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 2] = ['d' as i32 as symbol, 't' as i32 as symbol];
static mut s_1_1: [symbol; 2] = ['v' as i32 as symbol, 't' as i32 as symbol];
static mut a_1: [among; 2] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 3] = [
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_2_1: [symbol; 4] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_2_2: [symbol; 2] = ['i' as i32 as symbol, 'g' as i32 as symbol];
static mut s_2_3: [symbol; 3] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_2_4: [symbol; 3] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_2_5: [symbol; 4] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_2_6: [symbol; 3] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_7: [symbol; 3] = [
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'v' as i32 as symbol,
];
static mut s_2_8: [symbol; 4] = [
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'v' as i32 as symbol,
];
static mut s_2_9: [symbol; 4] = [
    's' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'v' as i32 as symbol,
];
static mut s_2_10: [symbol; 7] = [
    'h' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
    'l' as i32 as symbol,
    'o' as i32 as symbol,
    'v' as i32 as symbol,
];
static mut a_2: [among; 11] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
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
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: 7 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 19] = [
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
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    48 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    128 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut g_s_ending: [::core::ffi::c_uchar; 4] = [
    119 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    125 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    149 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
unsafe fn r_mark_regions(mut z: *mut SN_env) -> ::core::ffi::c_int {
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c_test1: ::core::ffi::c_int = (*z).c;
    (*z).c = (*z).c + 3 as ::core::ffi::c_int;
    if (*z).c > (*z).l {
        return 0 as ::core::ffi::c_int;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
    (*z).c = c_test1;
    if out_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        248 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    ) < 0 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = in_grouping(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        248 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if ret < 0 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    (*z).c += ret;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
    if !(*(*z).I.offset(1 as ::core::ffi::c_int as isize)
        >= *(*z).I.offset(0 as ::core::ffi::c_int as isize))
    {
        *(*z).I.offset(1 as ::core::ffi::c_int as isize) = *(*z)
            .I
            .offset(0 as ::core::ffi::c_int as isize);
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_main_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 1851426 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_0 as *const among,
        29 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
            if in_grouping_b(
                z,
                &raw const g_s_ending as *const ::core::ffi::c_uchar,
                98 as ::core::ffi::c_int,
                122 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                (*z).c = (*z).l - m2;
                if (*z).c <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                        as ::core::ffi::c_int != 'k' as i32
                {
                    return 0 as ::core::ffi::c_int;
                }
                (*z).c -= 1;
                if out_grouping_b(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    248 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0
                {
                    return 0 as ::core::ffi::c_int;
                }
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_0 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_consonant_pair(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut m_test1: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut mlimit2: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit2 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 116 as ::core::ffi::c_int
    {
        (*z).lb = mlimit2;
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_1 as *const among, 2 as ::core::ffi::c_int) == 0 {
        (*z).lb = mlimit2;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit2;
    (*z).c = (*z).l - m_test1;
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
unsafe fn r_other_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(1 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(1 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 4718720 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_2 as *const among, 11 as ::core::ffi::c_int) == 0 {
        (*z).lb = mlimit1;
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    (*z).lb = mlimit1;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn norwegian_ISO_8859_1_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_mark_regions(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = c1;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_main_suffix(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).l - m2;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_consonant_pair(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m3;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_other_suffix(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m4;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn norwegian_ISO_8859_1_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 2 as ::core::ffi::c_int);
}
pub unsafe fn norwegian_ISO_8859_1_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
