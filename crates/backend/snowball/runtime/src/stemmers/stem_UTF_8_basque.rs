use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 4] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_1: [symbol; 5] = [
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_2: [symbol; 5] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_3: [symbol; 5] = [
    'p' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_4: [symbol; 6] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_5: [symbol; 5] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_6: [symbol; 6] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_7: [symbol; 7] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_8: [symbol; 5] = [
    'g' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_9: [symbol; 5] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_10: [symbol; 5] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'g' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_11: [symbol; 4] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_12: [symbol; 5] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_13: [symbol; 6] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_14: [symbol; 5] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_15: [symbol; 6] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_16: [symbol; 6] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_17: [symbol; 2] = ['k' as i32 as symbol, 'a' as i32 as symbol];
static mut s_0_18: [symbol; 5] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_19: [symbol; 2] = ['l' as i32 as symbol, 'a' as i32 as symbol];
static mut s_0_20: [symbol; 4] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_21: [symbol; 4] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_22: [symbol; 4] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_23: [symbol; 5] = [
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_24: [symbol; 6] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_25: [symbol; 4] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_26: [symbol; 4] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_27: [symbol; 6] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_28: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_29: [symbol; 4] = [
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_30: [symbol; 7] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_31: [symbol; 4] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_32: [symbol; 4] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_33: [symbol; 4] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_34: [symbol; 5] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_35: [symbol; 4] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_36: [symbol; 4] = [
    'g' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_37: [symbol; 4] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_38: [symbol; 4] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_39: [symbol; 3] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_40: [symbol; 4] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_41: [symbol; 6] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_42: [symbol; 3] = [
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_43: [symbol; 6] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_44: [symbol; 3] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_45: [symbol; 6] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_46: [symbol; 6] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_47: [symbol; 6] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_48: [symbol; 3] = [
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_49: [symbol; 4] = [
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_50: [symbol; 4] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_51: [symbol; 4] = [
    'p' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_52: [symbol; 5] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_53: [symbol; 5] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_54: [symbol; 5] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_55: [symbol; 2] = ['l' as i32 as symbol, 'e' as i32 as symbol];
static mut s_0_56: [symbol; 4] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_57: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_58: [symbol; 6] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_59: [symbol; 4] = [
    'g' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_60: [symbol; 4] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_61: [symbol; 3] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_62: [symbol; 4] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_63: [symbol; 3] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_64: [symbol; 4] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_65: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_66: [symbol; 2] = ['k' as i32 as symbol, 'i' as i32 as symbol];
static mut s_0_67: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_68: [symbol; 4] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_69: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_70: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_71: [symbol; 5] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_72: [symbol; 5] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_73: [symbol; 5] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_74: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_75: [symbol; 6] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_76: [symbol; 2] = ['a' as i32 as symbol, 'n' as i32 as symbol];
static mut s_0_77: [symbol; 3] = [
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_78: [symbol; 4] = [
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_79: [symbol; 3] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_80: [symbol; 4] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_81: [symbol; 7] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_82: [symbol; 3] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_83: [symbol; 3] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_84: [symbol; 3] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_85: [symbol; 5] = [
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_86: [symbol; 4] = [
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_87: [symbol; 5] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_88: [symbol; 3] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_89: [symbol; 5] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_90: [symbol; 2] = ['g' as i32 as symbol, 'o' as i32 as symbol];
static mut s_0_91: [symbol; 3] = [
    'a' as i32 as symbol,
    'g' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_0_92: [symbol; 3] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_0_93: [symbol; 4] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_0_94: [symbol; 2] = ['o' as i32 as symbol, 'r' as i32 as symbol];
static mut s_0_95: [symbol; 3] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_0_96: [symbol; 4] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_0_97: [symbol; 2] = ['d' as i32 as symbol, 'u' as i32 as symbol];
static mut s_0_98: [symbol; 5] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_0_99: [symbol; 2] = ['t' as i32 as symbol, 'u' as i32 as symbol];
static mut s_0_100: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_0_101: [symbol; 6] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_0_102: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_0_103: [symbol; 6] = [
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_0_104: [symbol; 2] = ['e' as i32 as symbol, 'z' as i32 as symbol];
static mut s_0_105: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_0_106: [symbol; 4] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_0_107: [symbol; 5] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_0_108: [symbol; 5] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut a_0: [among; 109] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
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
            s_size: 5 as ::core::ffi::c_int,
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
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_24 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_29 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_31 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_32 as *const symbol,
            substring_i: 28 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_34 as *const symbol,
            substring_i: 33 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_40 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_43 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_45 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_46 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_47 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_49 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_50 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_51 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_53 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_55 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_56 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_57 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_58 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_59 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_61 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_62 as *const symbol,
            substring_i: 61 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_63 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_64 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_65 as *const symbol,
            substring_i: 64 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_66 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_67 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_68 as *const symbol,
            substring_i: 67 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_69 as *const symbol,
            substring_i: 67 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_70 as *const symbol,
            substring_i: 67 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_71 as *const symbol,
            substring_i: 70 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_72 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_73 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_74 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_75 as *const symbol,
            substring_i: 74 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_76 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_77 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_78 as *const symbol,
            substring_i: 77 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_79 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_80 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_81 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_82 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_83 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_84 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_85 as *const symbol,
            substring_i: 84 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_86 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_87 as *const symbol,
            substring_i: 86 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_88 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_89 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_90 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_91 as *const symbol,
            substring_i: 90 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_92 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_93 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_94 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_95 as *const symbol,
            substring_i: 94 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_96 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_97 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_98 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_99 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_100 as *const symbol,
            substring_i: 99 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_101 as *const symbol,
            substring_i: 100 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_102 as *const symbol,
            substring_i: 100 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_103 as *const symbol,
            substring_i: 99 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_104 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_105 as *const symbol,
            substring_i: 104 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_106 as *const symbol,
            substring_i: 104 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_107 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_108 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 3] = [
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_1: [symbol; 4] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_2: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_3: [symbol; 5] = [
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_4: [symbol; 5] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_5: [symbol; 5] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_6: [symbol; 5] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_7: [symbol; 6] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_8: [symbol; 6] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_9: [symbol; 5] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_10: [symbol; 5] = [
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_11: [symbol; 6] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_12: [symbol; 5] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_13: [symbol; 4] = [
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_14: [symbol; 5] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_15: [symbol; 3] = [
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_16: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_17: [symbol; 6] = [
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_18: [symbol; 4] = [
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_19: [symbol; 5] = [
    'g' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_20: [symbol; 3] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_21: [symbol; 6] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_22: [symbol; 3] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_23: [symbol; 5] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_24: [symbol; 5] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_25: [symbol; 5] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_26: [symbol; 5] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'x' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_27: [symbol; 2] = ['g' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_28: [symbol; 4] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_29: [symbol; 4] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_30: [symbol; 5] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_31: [symbol; 6] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_32: [symbol; 6] = [
    'h' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_33: [symbol; 6] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_34: [symbol; 4] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_35: [symbol; 4] = [
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_36: [symbol; 5] = [
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_37: [symbol; 5] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_38: [symbol; 5] = [
    'n' as i32 as symbol,
    'a' as i32 as symbol,
    'h' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_39: [symbol; 4] = [
    'o' as i32 as symbol,
    'h' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_40: [symbol; 3] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_41: [symbol; 5] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_42: [symbol; 3] = [
    'o' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_43: [symbol; 4] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_44: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_45: [symbol; 5] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_46: [symbol; 5] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_47: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_48: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_49: [symbol; 5] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_50: [symbol; 5] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_51: [symbol; 6] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_52: [symbol; 6] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_53: [symbol; 6] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_54: [symbol; 5] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_55: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_56: [symbol; 3] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_57: [symbol; 4] = [
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_58: [symbol; 5] = [
    'b' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_59: [symbol; 6] = [
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_60: [symbol; 2] = ['k' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_61: [symbol; 4] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_62: [symbol; 5] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_63: [symbol; 3] = [
    's' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_64: [symbol; 3] = [
    'x' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_65: [symbol; 3] = [
    'z' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_66: [symbol; 6] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_67: [symbol; 4] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_68: [symbol; 5] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_69: [symbol; 5] = [
    's' as i32 as symbol,
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_70: [symbol; 4] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_71: [symbol; 3] = [
    'o' as i32 as symbol,
    'l' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_72: [symbol; 2] = ['n' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_73: [symbol; 4] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_74: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_75: [symbol; 7] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_76: [symbol; 7] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_77: [symbol; 6] = [
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_78: [symbol; 5] = [
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_79: [symbol; 6] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_80: [symbol; 4] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_81: [symbol; 4] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_82: [symbol; 5] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_83: [symbol; 3] = [
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_84: [symbol; 4] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_85: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_86: [symbol; 6] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_87: [symbol; 5] = [
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_88: [symbol; 6] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_89: [symbol; 4] = [
    'n' as i32 as symbol,
    'g' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_90: [symbol; 4] = [
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_91: [symbol; 3] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_92: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_93: [symbol; 4] = [
    'z' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_94: [symbol; 3] = [
    'n' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_95: [symbol; 5] = [
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_96: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_97: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_98: [symbol; 5] = [
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_99: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_100: [symbol; 4] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_101: [symbol; 4] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_102: [symbol; 3] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_103: [symbol; 4] = [
    't' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_104: [symbol; 4] = [
    'z' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_105: [symbol; 4] = [
    't' as i32 as symbol,
    'x' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_106: [symbol; 4] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_107: [symbol; 4] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_108: [symbol; 2] = ['r' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_109: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_110: [symbol; 4] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_111: [symbol; 5] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_112: [symbol; 5] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_113: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_114: [symbol; 5] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_115: [symbol; 5] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_116: [symbol; 4] = [
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_117: [symbol; 4] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_118: [symbol; 4] = [
    'p' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_119: [symbol; 3] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_120: [symbol; 6] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_121: [symbol; 5] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_122: [symbol; 3] = [
    't' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_123: [symbol; 2] = ['s' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_124: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_125: [symbol; 2] = ['t' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_126: [symbol; 3] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_127: [symbol; 4] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_128: [symbol; 3] = [
    's' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_129: [symbol; 3] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_130: [symbol; 6] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_131: [symbol; 5] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_132: [symbol; 5] = [
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_133: [symbol; 5] = [
    'b' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_134: [symbol; 5] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_135: [symbol; 4] = [
    't' as i32 as symbol,
    's' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_136: [symbol; 3] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_137: [symbol; 6] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_138: [symbol; 5] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_139: [symbol; 4] = [
    't' as i32 as symbol,
    'x' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_140: [symbol; 3] = [
    'z' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_141: [symbol; 4] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'u' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_142: [symbol; 2] = ['z' as i32 as symbol, 'a' as i32 as symbol];
static mut s_1_143: [symbol; 3] = [
    'e' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_144: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_145: [symbol; 3] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_146: [symbol; 6] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_147: [symbol; 5] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_148: [symbol; 6] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_149: [symbol; 6] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_150: [symbol; 6] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_1_151: [symbol; 4] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_152: [symbol; 4] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_153: [symbol; 4] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_154: [symbol; 4] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_155: [symbol; 5] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_156: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_157: [symbol; 4] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_158: [symbol; 2] = ['g' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_159: [symbol; 4] = [
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_160: [symbol; 5] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_161: [symbol; 4] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_162: [symbol; 3] = [
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_163: [symbol; 4] = [
    'k' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_164: [symbol; 2] = ['n' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_165: [symbol; 5] = [
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_166: [symbol; 3] = [
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_167: [symbol; 4] = [
    'g' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_168: [symbol; 2] = ['p' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_169: [symbol; 5] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_170: [symbol; 2] = ['t' as i32 as symbol, 'e' as i32 as symbol];
static mut s_1_171: [symbol; 4] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_172: [symbol; 4] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_173: [symbol; 4] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_174: [symbol; 4] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'x' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_1_175: [symbol; 3] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_176: [symbol; 2] = ['d' as i32 as symbol, 'i' as i32 as symbol];
static mut s_1_177: [symbol; 4] = [
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_178: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_179: [symbol; 5] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_180: [symbol; 5] = [
    'h' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_181: [symbol; 5] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_182: [symbol; 3] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_183: [symbol; 3] = [
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_184: [symbol; 4] = [
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_185: [symbol; 4] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'g' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_186: [symbol; 4] = [
    'n' as i32 as symbol,
    'a' as i32 as symbol,
    'h' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_187: [symbol; 3] = [
    'o' as i32 as symbol,
    'h' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_188: [symbol; 2] = ['k' as i32 as symbol, 'i' as i32 as symbol];
static mut s_1_189: [symbol; 4] = [
    't' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_190: [symbol; 2] = ['o' as i32 as symbol, 'i' as i32 as symbol];
static mut s_1_191: [symbol; 3] = [
    'g' as i32 as symbol,
    'o' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_192: [symbol; 3] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_193: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_194: [symbol; 4] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_195: [symbol; 4] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_196: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_197: [symbol; 5] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_198: [symbol; 5] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_199: [symbol; 5] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_200: [symbol; 4] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_201: [symbol; 3] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_202: [symbol; 2] = ['t' as i32 as symbol, 'i' as i32 as symbol];
static mut s_1_203: [symbol; 5] = [
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_1_204: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
];
static mut s_1_205: [symbol; 2] = ['a' as i32 as symbol, 'k' as i32 as symbol];
static mut s_1_206: [symbol; 2] = ['e' as i32 as symbol, 'k' as i32 as symbol];
static mut s_1_207: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_1_208: [symbol; 5] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_1_209: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_1_210: [symbol; 4] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_1_211: [symbol; 3] = [
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_212: [symbol; 3] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_213: [symbol; 4] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_214: [symbol; 2] = ['e' as i32 as symbol, 'n' as i32 as symbol];
static mut s_1_215: [symbol; 3] = [
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_216: [symbol; 6] = [
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_217: [symbol; 6] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_218: [symbol; 5] = [
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'r' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_219: [symbol; 3] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_220: [symbol; 4] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_221: [symbol; 4] = [
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_222: [symbol; 5] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_223: [symbol; 3] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_224: [symbol; 3] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_225: [symbol; 3] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_226: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_227: [symbol; 5] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_228: [symbol; 5] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'z' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_229: [symbol; 4] = [
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_230: [symbol; 5] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_231: [symbol; 2] = ['g' as i32 as symbol, 'o' as i32 as symbol];
static mut s_1_232: [symbol; 3] = [
    'n' as i32 as symbol,
    'g' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_233: [symbol; 3] = [
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_234: [symbol; 2] = ['k' as i32 as symbol, 'o' as i32 as symbol];
static mut s_1_235: [symbol; 5] = [
    't' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_236: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_237: [symbol; 5] = [
    'e' as i32 as symbol,
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_238: [symbol; 3] = [
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_239: [symbol; 6] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_240: [symbol; 3] = [
    's' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_241: [symbol; 4] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_242: [symbol; 8] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_243: [symbol; 3] = [
    'z' as i32 as symbol,
    'k' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_244: [symbol; 2] = ['n' as i32 as symbol, 'o' as i32 as symbol];
static mut s_1_245: [symbol; 4] = [
    'z' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_246: [symbol; 2] = ['r' as i32 as symbol, 'o' as i32 as symbol];
static mut s_1_247: [symbol; 3] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_248: [symbol; 5] = [
    'i' as i32 as symbol,
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_249: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_250: [symbol; 4] = [
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_251: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_252: [symbol; 4] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_253: [symbol; 3] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_254: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_255: [symbol; 2] = ['t' as i32 as symbol, 'o' as i32 as symbol];
static mut s_1_256: [symbol; 3] = [
    't' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_257: [symbol; 3] = [
    'z' as i32 as symbol,
    't' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_258: [symbol; 3] = [
    't' as i32 as symbol,
    'x' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_259: [symbol; 3] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_260: [symbol; 6] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_1_261: [symbol; 3] = [
    0xc3 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
    'o' as i32 as symbol,
];
static mut s_1_262: [symbol; 2] = ['z' as i32 as symbol, 'p' as i32 as symbol];
static mut s_1_263: [symbol; 2] = ['a' as i32 as symbol, 'r' as i32 as symbol];
static mut s_1_264: [symbol; 3] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_1_265: [symbol; 5] = [
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'h' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_1_266: [symbol; 5] = [
    'z' as i32 as symbol,
    'e' as i32 as symbol,
    'h' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_1_267: [symbol; 4] = [
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_1_268: [symbol; 4] = [
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_1_269: [symbol; 3] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_1_270: [symbol; 4] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_1_271: [symbol; 2] = ['o' as i32 as symbol, 'r' as i32 as symbol];
static mut s_1_272: [symbol; 3] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_1_273: [symbol; 2] = ['o' as i32 as symbol, 's' as i32 as symbol];
static mut s_1_274: [symbol; 3] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_275: [symbol; 2] = ['d' as i32 as symbol, 'u' as i32 as symbol];
static mut s_1_276: [symbol; 5] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'd' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_277: [symbol; 4] = [
    'o' as i32 as symbol,
    'r' as i32 as symbol,
    'd' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_278: [symbol; 4] = [
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'k' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_279: [symbol; 4] = [
    'b' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_280: [symbol; 4] = [
    'd' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_281: [symbol; 3] = [
    't' as i32 as symbol,
    's' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_282: [symbol; 2] = ['t' as i32 as symbol, 'u' as i32 as symbol];
static mut s_1_283: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_284: [symbol; 5] = [
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_285: [symbol; 4] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_286: [symbol; 3] = [
    't' as i32 as symbol,
    'x' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_287: [symbol; 2] = ['z' as i32 as symbol, 'u' as i32 as symbol];
static mut s_1_288: [symbol; 3] = [
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_289: [symbol; 6] = [
    'g' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_290: [symbol; 1] = ['z' as i32 as symbol];
static mut s_1_291: [symbol; 2] = ['e' as i32 as symbol, 'z' as i32 as symbol];
static mut s_1_292: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut s_1_293: [symbol; 2] = ['t' as i32 as symbol, 'z' as i32 as symbol];
static mut s_1_294: [symbol; 5] = [
    'k' as i32 as symbol,
    'o' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'z' as i32 as symbol,
];
static mut a_1: [among; 295] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
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
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_8 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
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
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_17 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_18 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_19 as *const symbol,
            substring_i: 18 as ::core::ffi::c_int,
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_23 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_24 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_25 as *const symbol,
            substring_i: 22 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_28 as *const symbol,
            substring_i: 27 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_31 as *const symbol,
            substring_i: 30 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_36 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_37 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_41 as *const symbol,
            substring_i: 40 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_43 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_45 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_46 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_47 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_49 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_50 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_51 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_53 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_55 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_56 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
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
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_58 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_59 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_61 as *const symbol,
            substring_i: 60 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_62 as *const symbol,
            substring_i: 60 as ::core::ffi::c_int,
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_63 as *const symbol,
            substring_i: 60 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_64 as *const symbol,
            substring_i: 60 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_65 as *const symbol,
            substring_i: 60 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_66 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_67 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_68 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_69 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_70 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_71 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_72 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_73 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_74 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_1_75 as *const symbol,
            substring_i: 74 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_1_76 as *const symbol,
            substring_i: 74 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_77 as *const symbol,
            substring_i: 74 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_78 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_79 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_80 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_81 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_82 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_83 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_84 as *const symbol,
            substring_i: 83 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_85 as *const symbol,
            substring_i: 83 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_86 as *const symbol,
            substring_i: 85 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_87 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_88 as *const symbol,
            substring_i: 87 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_89 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_90 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_91 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_92 as *const symbol,
            substring_i: 91 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_93 as *const symbol,
            substring_i: 91 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_94 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_95 as *const symbol,
            substring_i: 94 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_96 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_97 as *const symbol,
            substring_i: 96 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_98 as *const symbol,
            substring_i: 96 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_99 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_100 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_101 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_102 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_103 as *const symbol,
            substring_i: 102 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_104 as *const symbol,
            substring_i: 102 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_105 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_106 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_107 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_108 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_109 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_110 as *const symbol,
            substring_i: 109 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_111 as *const symbol,
            substring_i: 109 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_112 as *const symbol,
            substring_i: 109 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_113 as *const symbol,
            substring_i: 109 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_114 as *const symbol,
            substring_i: 113 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_115 as *const symbol,
            substring_i: 109 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_116 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_117 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_118 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_119 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_120 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_121 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_122 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_123 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_124 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_125 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_126 as *const symbol,
            substring_i: 125 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_127 as *const symbol,
            substring_i: 126 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_128 as *const symbol,
            substring_i: 125 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_129 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_130 as *const symbol,
            substring_i: 129 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_131 as *const symbol,
            substring_i: 129 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_132 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_133 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_134 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_135 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_136 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_137 as *const symbol,
            substring_i: 136 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_138 as *const symbol,
            substring_i: 136 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_139 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_140 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_141 as *const symbol,
            substring_i: 140 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_142 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_143 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_144 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_145 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_146 as *const symbol,
            substring_i: 145 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_147 as *const symbol,
            substring_i: 145 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_148 as *const symbol,
            substring_i: 145 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_149 as *const symbol,
            substring_i: 145 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_150 as *const symbol,
            substring_i: 145 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_151 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_152 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_153 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_154 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_155 as *const symbol,
            substring_i: 154 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_156 as *const symbol,
            substring_i: 154 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_157 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_158 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_159 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_160 as *const symbol,
            substring_i: 159 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_161 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_162 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_163 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_164 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_165 as *const symbol,
            substring_i: 164 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_166 as *const symbol,
            substring_i: 164 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_167 as *const symbol,
            substring_i: 166 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_168 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_169 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_170 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_171 as *const symbol,
            substring_i: 170 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_172 as *const symbol,
            substring_i: 170 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_173 as *const symbol,
            substring_i: 170 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_174 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_175 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_176 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_177 as *const symbol,
            substring_i: 176 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_178 as *const symbol,
            substring_i: 177 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_179 as *const symbol,
            substring_i: 176 as ::core::ffi::c_int,
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_180 as *const symbol,
            substring_i: 176 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_181 as *const symbol,
            substring_i: 176 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_182 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_183 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_184 as *const symbol,
            substring_i: 183 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_185 as *const symbol,
            substring_i: 183 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_186 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_187 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_188 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_189 as *const symbol,
            substring_i: 188 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_190 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_191 as *const symbol,
            substring_i: 190 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_192 as *const symbol,
            substring_i: 190 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_193 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_194 as *const symbol,
            substring_i: 193 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_195 as *const symbol,
            substring_i: 193 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_196 as *const symbol,
            substring_i: 193 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_197 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_198 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_199 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_200 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_201 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_202 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_203 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_204 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_205 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_206 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_207 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_208 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_209 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_210 as *const symbol,
            substring_i: 209 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_211 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_212 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_213 as *const symbol,
            substring_i: 212 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_214 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_215 as *const symbol,
            substring_i: 214 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_216 as *const symbol,
            substring_i: 215 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_217 as *const symbol,
            substring_i: 215 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_218 as *const symbol,
            substring_i: 215 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_219 as *const symbol,
            substring_i: 214 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_220 as *const symbol,
            substring_i: 214 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_221 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_222 as *const symbol,
            substring_i: 221 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_223 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_224 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_225 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_226 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_227 as *const symbol,
            substring_i: 226 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_228 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_229 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_230 as *const symbol,
            substring_i: 229 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_231 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_232 as *const symbol,
            substring_i: 231 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_233 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_234 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_235 as *const symbol,
            substring_i: 234 as ::core::ffi::c_int,
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_236 as *const symbol,
            substring_i: 234 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_237 as *const symbol,
            substring_i: 236 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_238 as *const symbol,
            substring_i: 234 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_239 as *const symbol,
            substring_i: 234 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_240 as *const symbol,
            substring_i: 234 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_241 as *const symbol,
            substring_i: 234 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 8 as ::core::ffi::c_int,
            s: &raw const s_1_242 as *const symbol,
            substring_i: 241 as ::core::ffi::c_int,
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_243 as *const symbol,
            substring_i: 234 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_244 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_245 as *const symbol,
            substring_i: 244 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_246 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_247 as *const symbol,
            substring_i: 246 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_248 as *const symbol,
            substring_i: 247 as ::core::ffi::c_int,
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_249 as *const symbol,
            substring_i: 247 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_250 as *const symbol,
            substring_i: 247 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_251 as *const symbol,
            substring_i: 246 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_252 as *const symbol,
            substring_i: 246 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_253 as *const symbol,
            substring_i: 246 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_254 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_255 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_256 as *const symbol,
            substring_i: 255 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_257 as *const symbol,
            substring_i: 255 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_258 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_259 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_260 as *const symbol,
            substring_i: 259 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_261 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_262 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_263 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_264 as *const symbol,
            substring_i: 263 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_265 as *const symbol,
            substring_i: 263 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_266 as *const symbol,
            substring_i: 263 as ::core::ffi::c_int,
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_267 as *const symbol,
            substring_i: 263 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_268 as *const symbol,
            substring_i: 263 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_269 as *const symbol,
            substring_i: 263 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_270 as *const symbol,
            substring_i: 263 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_271 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_272 as *const symbol,
            substring_i: 271 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_273 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_274 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_275 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_276 as *const symbol,
            substring_i: 275 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_277 as *const symbol,
            substring_i: 275 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_278 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_279 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_280 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_281 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_282 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_283 as *const symbol,
            substring_i: 282 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_284 as *const symbol,
            substring_i: 282 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_285 as *const symbol,
            substring_i: 282 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_286 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_287 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_288 as *const symbol,
            substring_i: 287 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_289 as *const symbol,
            substring_i: 288 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_1_290 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_291 as *const symbol,
            substring_i: 290 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_292 as *const symbol,
            substring_i: 290 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_293 as *const symbol,
            substring_i: 290 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_294 as *const symbol,
            substring_i: 293 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 4] = [
    'z' as i32 as symbol,
    'l' as i32 as symbol,
    'e' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_1: [symbol; 5] = [
    'k' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_2: [symbol; 2] = ['l' as i32 as symbol, 'a' as i32 as symbol];
static mut s_2_3: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2_4: [symbol; 4] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_5: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_6: [symbol; 4] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_7: [symbol; 4] = [
    't' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_8: [symbol; 2] = ['g' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_9: [symbol; 2] = ['k' as i32 as symbol, 'i' as i32 as symbol];
static mut s_2_10: [symbol; 2] = ['i' as i32 as symbol, 'k' as i32 as symbol];
static mut s_2_11: [symbol; 5] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_2_12: [symbol; 3] = [
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_2_13: [symbol; 5] = [
    'l' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_2_14: [symbol; 4] = [
    'z' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_2_15: [symbol; 2] = ['g' as i32 as symbol, 'o' as i32 as symbol];
static mut s_2_16: [symbol; 2] = ['r' as i32 as symbol, 'o' as i32 as symbol];
static mut s_2_17: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_2_18: [symbol; 2] = ['t' as i32 as symbol, 'o' as i32 as symbol];
static mut a_2: [among; 19] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
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
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_12 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_14 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_2_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 3] = [
    17 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    65 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 7] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
    'd' as i32 as symbol,
    'e' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1: [symbol; 7] = [
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
    'b' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_2: [symbol; 6] = [
    'b' as i32 as symbol,
    'a' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_3: [symbol; 3] = [
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_4: [symbol; 3] = [
    't' as i32 as symbol,
    'r' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_5: [symbol; 6] = [
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_6: [symbol; 5] = [
    'z' as i32 as symbol,
    'e' as i32 as symbol,
    'h' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
];
static mut s_7: [symbol; 5] = [
    'g' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    'd' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_8: [symbol; 5] = [
    'i' as i32 as symbol,
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'r' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_9: [symbol; 5] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    'r' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_10: [symbol; 1] = ['z' as i32 as symbol];
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
        117 as ::core::ffi::c_int,
        0 as ::core::ffi::c_int,
    ) != 0
    {
        current_block = 9578573514911389373;
    } else {
        let mut c3: ::core::ffi::c_int = (*z).c;
        if out_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            117 as ::core::ffi::c_int,
            0 as ::core::ffi::c_int,
        ) != 0
        {
            current_block = 17859498464186878533;
        } else {
            let mut ret: ::core::ffi::c_int = out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                117 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                current_block = 17859498464186878533;
            } else {
                (*z).c += ret;
                current_block = 16064474001512691752;
            }
        }
        match current_block {
            16064474001512691752 => {}
            _ => {
                (*z).c = c3;
                if in_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    117 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0
                {
                    current_block = 9578573514911389373;
                } else {
                    let mut ret_0: ::core::ffi::c_int = in_grouping_U(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        117 as ::core::ffi::c_int,
                        1 as ::core::ffi::c_int,
                    );
                    if ret_0 < 0 as ::core::ffi::c_int {
                        current_block = 9578573514911389373;
                    } else {
                        (*z).c += ret_0;
                        current_block = 16064474001512691752;
                    }
                }
            }
        }
    }
    match current_block {
        9578573514911389373 => {
            (*z).c = c2;
            if out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                117 as ::core::ffi::c_int,
                0 as ::core::ffi::c_int,
            ) != 0
            {
                current_block = 10911004140690097988;
            } else {
                let mut c4: ::core::ffi::c_int = (*z).c;
                if out_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    117 as ::core::ffi::c_int,
                    0 as ::core::ffi::c_int,
                ) != 0
                {
                    current_block = 8379466528521760988;
                } else {
                    let mut ret_1: ::core::ffi::c_int = out_grouping_U(
                        z,
                        &raw const g_v as *const ::core::ffi::c_uchar,
                        97 as ::core::ffi::c_int,
                        117 as ::core::ffi::c_int,
                        1 as ::core::ffi::c_int,
                    );
                    if ret_1 < 0 as ::core::ffi::c_int {
                        current_block = 8379466528521760988;
                    } else {
                        (*z).c += ret_1;
                        current_block = 16064474001512691752;
                    }
                }
                match current_block {
                    16064474001512691752 => {}
                    _ => {
                        (*z).c = c4;
                        if in_grouping_U(
                            z,
                            &raw const g_v as *const ::core::ffi::c_uchar,
                            97 as ::core::ffi::c_int,
                            117 as ::core::ffi::c_int,
                            0 as ::core::ffi::c_int,
                        ) != 0
                        {
                            current_block = 10911004140690097988;
                        } else {
                            let mut ret_2: ::core::ffi::c_int = skip_utf8(
                                (*z).p,
                                (*z).c,
                                (*z).l,
                                1 as ::core::ffi::c_int,
                            );
                            if ret_2 < 0 as ::core::ffi::c_int {
                                current_block = 10911004140690097988;
                            } else {
                                (*z).c = ret_2;
                                current_block = 16064474001512691752;
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    match current_block {
        16064474001512691752 => {
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
        117 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret_3 < 0 as ::core::ffi::c_int) {
        (*z).c += ret_3;
        let mut ret_4: ::core::ffi::c_int = in_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            117 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_4 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_4;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = (*z).c;
            let mut ret_5: ::core::ffi::c_int = out_grouping_U(
                z,
                &raw const g_v as *const ::core::ffi::c_uchar,
                97 as ::core::ffi::c_int,
                117 as ::core::ffi::c_int,
                1 as ::core::ffi::c_int,
            );
            if !(ret_5 < 0 as ::core::ffi::c_int) {
                (*z).c += ret_5;
                let mut ret_6: ::core::ffi::c_int = in_grouping_U(
                    z,
                    &raw const g_v as *const ::core::ffi::c_uchar,
                    97 as ::core::ffi::c_int,
                    117 as ::core::ffi::c_int,
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
unsafe fn r_RV(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(2 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_R2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(0 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_R1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    return (*(*z).I.offset(1 as ::core::ffi::c_int as isize) <= (*z).c)
        as ::core::ffi::c_int;
}
unsafe fn r_aditzak(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 70566434 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_0 as *const among,
        109 as ::core::ffi::c_int,
    );
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
            let mut ret_1: ::core::ffi::c_int = r_R2(z);
            if ret_1 <= 0 as ::core::ffi::c_int {
                return ret_1;
            }
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        3 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                7 as ::core::ffi::c_int,
                &raw const s_0 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        4 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                7 as ::core::ffi::c_int,
                &raw const s_1 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        5 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_2 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_izenak(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 71162402 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_1 as *const among,
        295 as ::core::ffi::c_int,
    );
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
            let mut ret_1: ::core::ffi::c_int = r_R2(z);
            if ret_1 <= 0 as ::core::ffi::c_int {
                return ret_1;
            }
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        3 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_3 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        4 => {
            let mut ret_4: ::core::ffi::c_int = r_R1(z);
            if ret_4 <= 0 as ::core::ffi::c_int {
                return ret_4;
            }
            let mut ret_5: ::core::ffi::c_int = slice_del(z);
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        5 => {
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                3 as ::core::ffi::c_int,
                &raw const s_4 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        6 => {
            let mut ret_7: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_5 as *const symbol,
            );
            if ret_7 < 0 as ::core::ffi::c_int {
                return ret_7;
            }
        }
        7 => {
            let mut ret_8: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_6 as *const symbol,
            );
            if ret_8 < 0 as ::core::ffi::c_int {
                return ret_8;
            }
        }
        8 => {
            let mut ret_9: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret_9 < 0 as ::core::ffi::c_int {
                return ret_9;
            }
        }
        9 => {
            let mut ret_10: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_8 as *const symbol,
            );
            if ret_10 < 0 as ::core::ffi::c_int {
                return ret_10;
            }
        }
        10 => {
            let mut ret_11: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_9 as *const symbol,
            );
            if ret_11 < 0 as ::core::ffi::c_int {
                return ret_11;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_adjetiboak(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 35362 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_2 as *const among,
        19 as ::core::ffi::c_int,
    );
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
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_10 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn basque_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut ret: ::core::ffi::c_int = r_mark_regions(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    loop {
        let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_0: ::core::ffi::c_int = r_aditzak(z);
        if ret_0 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m1;
            break;
        } else if ret_0 < 0 as ::core::ffi::c_int {
            return ret_0
        }
    }
    loop {
        let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut ret_1: ::core::ffi::c_int = r_izenak(z);
        if ret_1 == 0 as ::core::ffi::c_int {
            (*z).c = (*z).l - m2;
            break;
        } else if ret_1 < 0 as ::core::ffi::c_int {
            return ret_1
        }
    }
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_adjetiboak(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m3;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn basque_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 3 as ::core::ffi::c_int);
}
pub unsafe fn basque_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
