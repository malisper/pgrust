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
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_0_1: [symbol; 3] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'h' as i32 as symbol,
];
static mut s_0_2: [symbol; 3] = [
    'p' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut a_0: [among; 3] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 3] = [
    'n' as i32 as symbol,
    'y' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_1: [symbol; 2] = ['k' as i32 as symbol, 'u' as i32 as symbol];
static mut s_1_2: [symbol; 2] = ['m' as i32 as symbol, 'u' as i32 as symbol];
static mut a_1: [among; 3] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
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
    ]
};
static mut s_2_0: [symbol; 1] = ['i' as i32 as symbol];
static mut s_2_1: [symbol; 2] = ['a' as i32 as symbol, 'n' as i32 as symbol];
static mut s_2_2: [symbol; 3] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut a_2: [among; 3] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: Some(
                r_SUFFIX_I_OK as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: Some(
                r_SUFFIX_AN_OK as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: Some(
                r_SUFFIX_KAN_OK
                    as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
    ]
};
static mut s_3_0: [symbol; 2] = ['d' as i32 as symbol, 'i' as i32 as symbol];
static mut s_3_1: [symbol; 2] = ['k' as i32 as symbol, 'e' as i32 as symbol];
static mut s_3_2: [symbol; 2] = ['m' as i32 as symbol, 'e' as i32 as symbol];
static mut s_3_3: [symbol; 3] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_4: [symbol; 3] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_5: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_3_6: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_3_7: [symbol; 3] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_3_8: [symbol; 3] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_3_9: [symbol; 4] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_3_10: [symbol; 4] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'y' as i32 as symbol,
];
static mut s_3_11: [symbol; 3] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut a_3: [among; 12] = unsafe {
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
            result: 2 as ::core::ffi::c_int,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_4 as *const symbol,
            substring_i: 2 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_6 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: Some(
                r_VOWEL as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_9 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_10 as *const symbol,
            substring_i: 8 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: Some(
                r_VOWEL as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 2] = ['b' as i32 as symbol, 'e' as i32 as symbol];
static mut s_4_1: [symbol; 7] = [
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_4_2: [symbol; 3] = [
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_4_3: [symbol; 2] = ['p' as i32 as symbol, 'e' as i32 as symbol];
static mut s_4_4: [symbol; 7] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_4_5: [symbol; 3] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut a_4: [among; 6] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: Some(
                r_KER as unsafe fn(*mut SN_env) -> ::core::ffi::c_int,
            ),
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_4_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_4_5 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_vowel: [::core::ffi::c_uchar; 3] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 2] = ['e' as i32 as symbol, 'r' as i32 as symbol];
static mut s_1: [symbol; 1] = ['s' as i32 as symbol];
static mut s_2: [symbol; 1] = ['s' as i32 as symbol];
static mut s_3: [symbol; 1] = ['p' as i32 as symbol];
static mut s_4: [symbol; 1] = ['p' as i32 as symbol];
static mut s_5: [symbol; 4] = [
    'a' as i32 as symbol,
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_6: [symbol; 4] = [
    'a' as i32 as symbol,
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
unsafe fn r_remove_particle(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 2 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 104 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 110 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_0 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_possessive_pronoun(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 97 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 117 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_1 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_SUFFIX_KAN_OK(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 3 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 2 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_SUFFIX_AN_OK(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(0 as ::core::ffi::c_int as isize) != 1 as ::core::ffi::c_int)
        as ::core::ffi::c_int;
}
unsafe fn r_SUFFIX_I_OK(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if *(*z).I.offset(0 as ::core::ffi::c_int as isize) > 2 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 's' as i32
    {
        (*z).c = (*z).l - m1;
    } else {
        (*z).c -= 1;
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_suffix(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 105 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 110 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_2 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_VOWEL(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if in_grouping(
        z,
        &raw const g_vowel as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        117 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_KER(mut z: *mut SN_env) -> ::core::ffi::c_int {
    if out_grouping(
        z,
        &raw const g_vowel as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        117 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        return 0 as ::core::ffi::c_int;
    }
    if eq_s(z, 2 as ::core::ffi::c_int, &raw const s_0 as *const symbol) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_first_order_prefix(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    if (*z).c + 1 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 105 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among(z, &raw const a_3 as *const among, 12 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 3 as ::core::ffi::c_int;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
        }
        3 => {
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_1 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
        }
        4 => {
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 3 as ::core::ffi::c_int;
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_2 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
        }
        5 => {
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
            let mut c1: ::core::ffi::c_int = (*z).c;
            let mut c2: ::core::ffi::c_int = (*z).c;
            if in_grouping(
                z,
                &raw const g_vowel as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                117 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                (*z).c = c1;
                let mut ret_4: ::core::ffi::c_int = slice_del(z);
                if ret_4 < 0 as ::core::ffi::c_int {
                    return ret_4;
                }
            } else {
                (*z).c = c2;
                let mut ret_3: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_3 as *const symbol,
                );
                if ret_3 < 0 as ::core::ffi::c_int {
                    return ret_3;
                }
            }
        }
        6 => {
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 3 as ::core::ffi::c_int;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
            let mut c3: ::core::ffi::c_int = (*z).c;
            let mut c4: ::core::ffi::c_int = (*z).c;
            if in_grouping(
                z,
                &raw const g_vowel as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                117 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                (*z).c = c3;
                let mut ret_6: ::core::ffi::c_int = slice_del(z);
                if ret_6 < 0 as ::core::ffi::c_int {
                    return ret_6;
                }
            } else {
                (*z).c = c4;
                let mut ret_5: ::core::ffi::c_int = slice_from_s(
                    z,
                    1 as ::core::ffi::c_int,
                    &raw const s_4 as *const symbol,
                );
                if ret_5 < 0 as ::core::ffi::c_int {
                    return ret_5;
                }
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_remove_second_order_prefix(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    if (*z).c + 1 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 101 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among(z, &raw const a_4 as *const among, 6 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 2 as ::core::ffi::c_int;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_5 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 4 as ::core::ffi::c_int;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
        }
        4 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_6 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 4 as ::core::ffi::c_int;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) -= 1 as ::core::ffi::c_int;
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn indonesian_ISO_8859_1_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut c1: ::core::ffi::c_int = (*z).c;
    loop {
        let mut c2: ::core::ffi::c_int = (*z).c;
        let mut ret: ::core::ffi::c_int = out_grouping(
            z,
            &raw const g_vowel as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            117 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if ret < 0 as ::core::ffi::c_int {
            (*z).c = c2;
            break;
        } else {
            (*z).c += ret;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) += 1 as ::core::ffi::c_int;
        }
    }
    (*z).c = c1;
    if *(*z).I.offset(1 as ::core::ffi::c_int as isize) <= 2 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_0: ::core::ffi::c_int = r_remove_particle(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).c = (*z).l - m3;
    if *(*z).I.offset(1 as ::core::ffi::c_int as isize) <= 2 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_1: ::core::ffi::c_int = r_remove_possessive_pronoun(z);
    if ret_1 < 0 as ::core::ffi::c_int {
        return ret_1;
    }
    (*z).c = (*z).l - m4;
    (*z).c = (*z).lb;
    if *(*z).I.offset(1 as ::core::ffi::c_int as isize) <= 2 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut c5: ::core::ffi::c_int = (*z).c;
    let mut c_test6: ::core::ffi::c_int = (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_remove_first_order_prefix(z);
    if ret_2 == 0 as ::core::ffi::c_int {
        (*z).c = c5;
        let mut c9: ::core::ffi::c_int = (*z).c;
        let mut ret_5: ::core::ffi::c_int = r_remove_second_order_prefix(z);
        if ret_5 < 0 as ::core::ffi::c_int {
            return ret_5;
        }
        (*z).c = c9;
        let mut c10: ::core::ffi::c_int = (*z).c;
        if !(*(*z).I.offset(1 as ::core::ffi::c_int as isize) <= 2 as ::core::ffi::c_int)
        {
            (*z).lb = (*z).c;
            (*z).c = (*z).l;
            let mut ret_6: ::core::ffi::c_int = r_remove_suffix(z);
            if !(ret_6 == 0 as ::core::ffi::c_int) {
                if ret_6 < 0 as ::core::ffi::c_int {
                    return ret_6;
                }
                (*z).c = (*z).lb;
            }
        }
        (*z).c = c10;
    } else {
        if ret_2 < 0 as ::core::ffi::c_int {
            return ret_2;
        }
        let mut c7: ::core::ffi::c_int = (*z).c;
        let mut c_test8: ::core::ffi::c_int = (*z).c;
        if !(*(*z).I.offset(1 as ::core::ffi::c_int as isize) <= 2 as ::core::ffi::c_int)
        {
            (*z).lb = (*z).c;
            (*z).c = (*z).l;
            let mut ret_3: ::core::ffi::c_int = r_remove_suffix(z);
            if !(ret_3 == 0 as ::core::ffi::c_int) {
                if ret_3 < 0 as ::core::ffi::c_int {
                    return ret_3;
                }
                (*z).c = (*z).lb;
                (*z).c = c_test8;
                if !(*(*z).I.offset(1 as ::core::ffi::c_int as isize)
                    <= 2 as ::core::ffi::c_int)
                {
                    let mut ret_4: ::core::ffi::c_int = r_remove_second_order_prefix(z);
                    if !(ret_4 == 0 as ::core::ffi::c_int) {
                        if ret_4 < 0 as ::core::ffi::c_int {
                            return ret_4;
                        }
                    }
                }
            }
        }
        (*z).c = c7;
        (*z).c = c_test6;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn indonesian_ISO_8859_1_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 2 as ::core::ffi::c_int);
}
pub unsafe fn indonesian_ISO_8859_1_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
