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
static mut s_0_1: [symbol; 2] = ['i' as i32 as symbol, 'a' as i32 as symbol];
static mut s_0_2: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_3: [symbol; 4] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_4: [symbol; 5] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_5: [symbol; 5] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_6: [symbol; 6] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_7: [symbol; 4] = [
    'y' as i32 as symbol,
    's' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_8: [symbol; 5] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'n' as i32 as symbol,
    'a' as i32 as symbol,
];
static mut s_0_9: [symbol; 1] = ['e' as i32 as symbol];
static mut s_0_10: [symbol; 2] = ['i' as i32 as symbol, 'e' as i32 as symbol];
static mut s_0_11: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_12: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_13: [symbol; 3] = [
    'o' as i32 as symbol,
    'j' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_14: [symbol; 4] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'j' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_15: [symbol; 3] = [
    'u' as i32 as symbol,
    'j' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_16: [symbol; 4] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'j' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_17: [symbol; 3] = [
    'y' as i32 as symbol,
    'j' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_18: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'y' as i32 as symbol,
    'j' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_19: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'y' as i32 as symbol,
    'j' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_20: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_21: [symbol; 3] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_22: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_23: [symbol; 4] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_24: [symbol; 3] = [
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_25: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_26: [symbol; 7] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_27: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_28: [symbol; 4] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_29: [symbol; 4] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_30: [symbol; 5] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_31: [symbol; 3] = [
    'y' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_32: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'y' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_33: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'y' as i32 as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_34: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_35: [symbol; 3] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_36: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_37: [symbol; 3] = [
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_38: [symbol; 4] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_39: [symbol; 4] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_40: [symbol; 3] = [
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_41: [symbol; 4] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_42: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_43: [symbol; 7] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_0_44: [symbol; 1] = ['i' as i32 as symbol];
static mut s_0_45: [symbol; 2] = ['a' as i32 as symbol, 'i' as i32 as symbol];
static mut s_0_46: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_47: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_48: [symbol; 2] = ['e' as i32 as symbol, 'i' as i32 as symbol];
static mut s_0_49: [symbol; 5] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_50: [symbol; 2] = ['k' as i32 as symbol, 'i' as i32 as symbol];
static mut s_0_51: [symbol; 3] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_52: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_53: [symbol; 3] = [
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_54: [symbol; 4] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_55: [symbol; 2] = ['s' as i32 as symbol, 'i' as i32 as symbol];
static mut s_0_56: [symbol; 3] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_57: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_58: [symbol; 3] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_59: [symbol; 4] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_60: [symbol; 5] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_61: [symbol; 3] = [
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_62: [symbol; 4] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_63: [symbol; 4] = [
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_64: [symbol; 7] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_65: [symbol; 4] = [
    'u' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_66: [symbol; 3] = [
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_67: [symbol; 6] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_68: [symbol; 4] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_69: [symbol; 5] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_70: [symbol; 6] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_71: [symbol; 3] = [
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_72: [symbol; 4] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_73: [symbol; 7] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_74: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_75: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_76: [symbol; 4] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_77: [symbol; 5] = [
    't' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_78: [symbol; 2] = ['t' as i32 as symbol, 'i' as i32 as symbol];
static mut s_0_79: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_80: [symbol; 4] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_81: [symbol; 3] = [
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_82: [symbol; 4] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_83: [symbol; 4] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_84: [symbol; 5] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_85: [symbol; 4] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_86: [symbol; 5] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_87: [symbol; 3] = [
    'y' as i32 as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_88: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_89: [symbol; 7] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'l' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_90: [symbol; 6] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_91: [symbol; 7] = [
    't' as i32 as symbol,
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_92: [symbol; 2] = ['u' as i32 as symbol, 'i' as i32 as symbol];
static mut s_0_93: [symbol; 3] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_94: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'i' as i32 as symbol,
];
static mut s_0_95: [symbol; 2] = ['o' as i32 as symbol, 'j' as i32 as symbol];
static mut s_0_96: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
];
static mut s_0_97: [symbol; 1] = ['k' as i32 as symbol];
static mut s_0_98: [symbol; 2] = ['a' as i32 as symbol, 'm' as i32 as symbol];
static mut s_0_99: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_0_100: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_0_101: [symbol; 2] = ['i' as i32 as symbol, 'm' as i32 as symbol];
static mut s_0_102: [symbol; 3] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_0_103: [symbol; 2] = ['o' as i32 as symbol, 'm' as i32 as symbol];
static mut s_0_104: [symbol; 3] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_0_105: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_0_106: [symbol; 6] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
];
static mut s_0_107: [symbol; 2] = ['a' as i32 as symbol, 'n' as i32 as symbol];
static mut s_0_108: [symbol; 2] = ['o' as i32 as symbol, 'n' as i32 as symbol];
static mut s_0_109: [symbol; 3] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_110: [symbol; 2] = ['u' as i32 as symbol, 'n' as i32 as symbol];
static mut s_0_111: [symbol; 3] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_0_112: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'n' as i32 as symbol,
];
static mut s_0_113: [symbol; 1] = ['o' as i32 as symbol];
static mut s_0_114: [symbol; 2] = ['i' as i32 as symbol, 'o' as i32 as symbol];
static mut s_0_115: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_0_116: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
    'o' as i32 as symbol,
];
static mut s_0_117: [symbol; 2] = ['u' as i32 as symbol, 'o' as i32 as symbol];
static mut s_0_118: [symbol; 1] = ['s' as i32 as symbol];
static mut s_0_119: [symbol; 2] = ['a' as i32 as symbol, 's' as i32 as symbol];
static mut s_0_120: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_121: [symbol; 2] = ['e' as i32 as symbol, 's' as i32 as symbol];
static mut s_0_122: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_123: [symbol; 2] = ['i' as i32 as symbol, 's' as i32 as symbol];
static mut s_0_124: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_125: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_126: [symbol; 6] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_127: [symbol; 4] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_128: [symbol; 6] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_129: [symbol; 4] = [
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_130: [symbol; 5] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_131: [symbol; 4] = [
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_132: [symbol; 5] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_133: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_134: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_135: [symbol; 4] = [
    'y' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_136: [symbol; 3] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_137: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_138: [symbol; 4] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_139: [symbol; 3] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_140: [symbol; 5] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_141: [symbol; 5] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_142: [symbol; 3] = [
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_143: [symbol; 4] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_144: [symbol; 3] = [
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_145: [symbol; 4] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_146: [symbol; 3] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_147: [symbol; 2] = ['o' as i32 as symbol, 's' as i32 as symbol];
static mut s_0_148: [symbol; 3] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_149: [symbol; 3] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_150: [symbol; 4] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_151: [symbol; 3] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_152: [symbol; 2] = ['u' as i32 as symbol, 's' as i32 as symbol];
static mut s_0_153: [symbol; 3] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_154: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_155: [symbol; 3] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_156: [symbol; 2] = ['y' as i32 as symbol, 's' as i32 as symbol];
static mut s_0_157: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'y' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_158: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    'y' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_0_159: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_160: [symbol; 4] = [
    'i' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_161: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_162: [symbol; 5] = [
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_163: [symbol; 6] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_164: [symbol; 5] = [
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_165: [symbol; 6] = [
    'k' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_166: [symbol; 6] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_167: [symbol; 5] = [
    'o' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_168: [symbol; 6] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_169: [symbol; 9] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_170: [symbol; 5] = [
    'a' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_171: [symbol; 6] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_172: [symbol; 6] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_173: [symbol; 5] = [
    'o' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_174: [symbol; 6] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_175: [symbol; 9] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_176: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_177: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_178: [symbol; 4] = [
    't' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_0_179: [symbol; 2] = ['a' as i32 as symbol, 't' as i32 as symbol];
static mut s_0_180: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_0_181: [symbol; 2] = ['i' as i32 as symbol, 't' as i32 as symbol];
static mut s_0_182: [symbol; 3] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_0_183: [symbol; 2] = ['o' as i32 as symbol, 't' as i32 as symbol];
static mut s_0_184: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
];
static mut s_0_185: [symbol; 6] = [
    't' as i32 as symbol,
    'u' as i32 as symbol,
    'm' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
];
static mut s_0_186: [symbol; 1] = ['u' as i32 as symbol];
static mut s_0_187: [symbol; 2] = ['a' as i32 as symbol, 'u' as i32 as symbol];
static mut s_0_188: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_0_189: [symbol; 5] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_0_190: [symbol; 2] = ['i' as i32 as symbol, 'u' as i32 as symbol];
static mut s_0_191: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_0_192: [symbol; 3] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_0_193: [symbol; 1] = ['y' as i32 as symbol];
static mut s_0_194: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_0_195: [symbol; 3] = [
    'i' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_0_196: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
];
static mut s_0_197: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
];
static mut s_0_198: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_0_199: [symbol; 4] = [
    'e' as i32 as symbol,
    'n' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_0_200: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_0_201: [symbol; 2] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_0_202: [symbol; 3] = [
    'i' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_0_203: [symbol; 4] = [
    'e' as i32 as symbol,
    'r' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 204] = unsafe {
    [
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: 3 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: 5 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_11 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_12 as *const symbol,
            substring_i: 10 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_13 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_14 as *const symbol,
            substring_i: 13 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_15 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_16 as *const symbol,
            substring_i: 15 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_17 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_18 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_19 as *const symbol,
            substring_i: 17 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_20 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_21 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_22 as *const symbol,
            substring_i: 21 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_23 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_24 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_25 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_26 as *const symbol,
            substring_i: 25 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_27 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_28 as *const symbol,
            substring_i: 27 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_29 as *const symbol,
            substring_i: 27 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_31 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_32 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_33 as *const symbol,
            substring_i: 31 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_34 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_35 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_36 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_37 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_38 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_39 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_40 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_41 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_42 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_43 as *const symbol,
            substring_i: 42 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_45 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_46 as *const symbol,
            substring_i: 45 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_47 as *const symbol,
            substring_i: 46 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_48 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_49 as *const symbol,
            substring_i: 48 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_50 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_51 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_52 as *const symbol,
            substring_i: 51 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_53 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_54 as *const symbol,
            substring_i: 53 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_55 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_56 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_57 as *const symbol,
            substring_i: 56 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_58 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_59 as *const symbol,
            substring_i: 58 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_60 as *const symbol,
            substring_i: 59 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_61 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_62 as *const symbol,
            substring_i: 61 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_63 as *const symbol,
            substring_i: 61 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_64 as *const symbol,
            substring_i: 63 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_65 as *const symbol,
            substring_i: 61 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_66 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_67 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_68 as *const symbol,
            substring_i: 66 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_69 as *const symbol,
            substring_i: 68 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_70 as *const symbol,
            substring_i: 69 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_71 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_72 as *const symbol,
            substring_i: 71 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_73 as *const symbol,
            substring_i: 72 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_74 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_75 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_76 as *const symbol,
            substring_i: 55 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_77 as *const symbol,
            substring_i: 76 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_78 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_79 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_80 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_81 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_82 as *const symbol,
            substring_i: 81 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_83 as *const symbol,
            substring_i: 81 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_84 as *const symbol,
            substring_i: 83 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_85 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_86 as *const symbol,
            substring_i: 85 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_87 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_88 as *const symbol,
            substring_i: 78 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_89 as *const symbol,
            substring_i: 88 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_90 as *const symbol,
            substring_i: 88 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_0_91 as *const symbol,
            substring_i: 88 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_92 as *const symbol,
            substring_i: 44 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_93 as *const symbol,
            substring_i: 92 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_94 as *const symbol,
            substring_i: 93 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_95 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_96 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_97 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_98 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_99 as *const symbol,
            substring_i: 98 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_100 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_101 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_102 as *const symbol,
            substring_i: 101 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_103 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_104 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_105 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_106 as *const symbol,
            substring_i: 105 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_107 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_108 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_109 as *const symbol,
            substring_i: 108 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_110 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_111 as *const symbol,
            substring_i: 110 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_112 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_113 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_114 as *const symbol,
            substring_i: 113 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_115 as *const symbol,
            substring_i: 114 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_116 as *const symbol,
            substring_i: 113 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_117 as *const symbol,
            substring_i: 113 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_118 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_119 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_120 as *const symbol,
            substring_i: 119 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_121 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_122 as *const symbol,
            substring_i: 121 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_123 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_124 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_125 as *const symbol,
            substring_i: 124 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_126 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_127 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_128 as *const symbol,
            substring_i: 127 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_129 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_130 as *const symbol,
            substring_i: 129 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_131 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_132 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_133 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_134 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_135 as *const symbol,
            substring_i: 123 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_136 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_137 as *const symbol,
            substring_i: 136 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_138 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_139 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_140 as *const symbol,
            substring_i: 139 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_141 as *const symbol,
            substring_i: 139 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_142 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_143 as *const symbol,
            substring_i: 142 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_144 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_145 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_146 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_147 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_148 as *const symbol,
            substring_i: 147 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_149 as *const symbol,
            substring_i: 147 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_150 as *const symbol,
            substring_i: 149 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_151 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_152 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_153 as *const symbol,
            substring_i: 152 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_154 as *const symbol,
            substring_i: 153 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_155 as *const symbol,
            substring_i: 152 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_156 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_157 as *const symbol,
            substring_i: 156 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_158 as *const symbol,
            substring_i: 156 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_159 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_160 as *const symbol,
            substring_i: 159 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_161 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_162 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_163 as *const symbol,
            substring_i: 162 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_164 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_165 as *const symbol,
            substring_i: 164 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_166 as *const symbol,
            substring_i: 164 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_167 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_168 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_0_169 as *const symbol,
            substring_i: 168 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_170 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_171 as *const symbol,
            substring_i: 170 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_172 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_173 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_174 as *const symbol,
            substring_i: 161 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 9 as ::core::ffi::c_int,
            s: &raw const s_0_175 as *const symbol,
            substring_i: 174 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_176 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_177 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_178 as *const symbol,
            substring_i: 118 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_179 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_180 as *const symbol,
            substring_i: 179 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_181 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_182 as *const symbol,
            substring_i: 181 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_183 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_184 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_0_185 as *const symbol,
            substring_i: 184 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_186 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_187 as *const symbol,
            substring_i: 186 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_188 as *const symbol,
            substring_i: 187 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_0_189 as *const symbol,
            substring_i: 188 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_190 as *const symbol,
            substring_i: 186 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_191 as *const symbol,
            substring_i: 190 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_192 as *const symbol,
            substring_i: 190 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 1 as ::core::ffi::c_int,
            s: &raw const s_0_193 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_194 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_195 as *const symbol,
            substring_i: 194 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_196 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_197 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_198 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_199 as *const symbol,
            substring_i: 198 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_200 as *const symbol,
            substring_i: 198 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_201 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_202 as *const symbol,
            substring_i: 201 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_0_203 as *const symbol,
            substring_i: 201 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    'g' as i32 as symbol,
];
static mut s_1_1: [symbol; 2] = ['a' as i32 as symbol, 'j' as i32 as symbol];
static mut s_1_2: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'j' as i32 as symbol,
];
static mut s_1_3: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    'j' as i32 as symbol,
];
static mut s_1_4: [symbol; 2] = ['o' as i32 as symbol, 'j' as i32 as symbol];
static mut s_1_5: [symbol; 3] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'j' as i32 as symbol,
];
static mut s_1_6: [symbol; 3] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    'j' as i32 as symbol,
];
static mut s_1_7: [symbol; 4] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    'j' as i32 as symbol,
];
static mut s_1_8: [symbol; 3] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    'j' as i32 as symbol,
];
static mut s_1_9: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
];
static mut s_1_10: [symbol; 4] = [
    'i' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
];
static mut s_1_11: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
];
static mut s_1_12: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
];
static mut s_1_13: [symbol; 4] = [
    'i' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
];
static mut s_1_14: [symbol; 2] = ['o' as i32 as symbol, 'k' as i32 as symbol];
static mut s_1_15: [symbol; 3] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_1_16: [symbol; 3] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_1_17: [symbol; 5] = [
    'u' as i32 as symbol,
    'l' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_1_18: [symbol; 6] = [
    'u' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'k' as i32 as symbol,
];
static mut s_1_19: [symbol; 4] = [
    'i' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'k' as i32 as symbol,
];
static mut s_1_20: [symbol; 3] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'l' as i32 as symbol,
];
static mut s_1_21: [symbol; 2] = ['y' as i32 as symbol, 'l' as i32 as symbol];
static mut s_1_22: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'l' as i32 as symbol,
];
static mut s_1_23: [symbol; 2] = ['a' as i32 as symbol, 'm' as i32 as symbol];
static mut s_1_24: [symbol; 3] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_1_25: [symbol; 3] = [
    'j' as i32 as symbol,
    'a' as i32 as symbol,
    'm' as i32 as symbol,
];
static mut s_1_26: [symbol; 4] = [
    'z' as i32 as symbol,
    'g' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_27: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_28: [symbol; 3] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'n' as i32 as symbol,
];
static mut s_1_29: [symbol; 2] = ['o' as i32 as symbol, 'p' as i32 as symbol];
static mut s_1_30: [symbol; 3] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    'p' as i32 as symbol,
];
static mut s_1_31: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_32: [symbol; 3] = [
    'i' as i32 as symbol,
    'e' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_33: [symbol; 3] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_34: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_35: [symbol; 2] = ['o' as i32 as symbol, 's' as i32 as symbol];
static mut s_1_36: [symbol; 3] = [
    'i' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_37: [symbol; 3] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_38: [symbol; 4] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_39: [symbol; 3] = [
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_40: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_1_41: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_1_42: [symbol; 4] = [
    'i' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_1_43: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_1_44: [symbol; 7] = [
    'u' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_45: [symbol; 3] = [
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_46: [symbol; 4] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_47: [symbol; 5] = [
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_48: [symbol; 3] = [
    'i' as i32 as symbol,
    'n' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_49: [symbol; 2] = ['o' as i32 as symbol, 't' as i32 as symbol];
static mut s_1_50: [symbol; 3] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_51: [symbol; 4] = [
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
];
static mut s_1_52: [symbol; 2] = ['y' as i32 as symbol, 't' as i32 as symbol];
static mut s_1_53: [symbol; 3] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
];
static mut s_1_54: [symbol; 5] = [
    'y' as i32 as symbol,
    'k' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    't' as i32 as symbol,
];
static mut s_1_55: [symbol; 3] = [
    'i' as i32 as symbol,
    'a' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut s_1_56: [symbol; 3] = [
    'd' as i32 as symbol,
    'a' as i32 as symbol,
    'v' as i32 as symbol,
];
static mut s_1_57: [symbol; 2] = ['s' as i32 as symbol, 'v' as i32 as symbol];
static mut s_1_58: [symbol; 3] = [
    0xc5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    'v' as i32 as symbol,
];
static mut s_1_59: [symbol; 6] = [
    'y' as i32 as symbol,
    'k' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_1_60: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
];
static mut s_1_61: [symbol; 5] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 62] = unsafe {
    [
        among {
            s_size: 3 as ::core::ffi::c_int,
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
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_2 as *const symbol,
            substring_i: 1 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_5 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_6 as *const symbol,
            substring_i: 4 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
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
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_10 as *const symbol,
            substring_i: 9 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_13 as *const symbol,
            substring_i: 12 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_15 as *const symbol,
            substring_i: 14 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_17 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_18 as *const symbol,
            substring_i: 16 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_24 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_25 as *const symbol,
            substring_i: 23 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_30 as *const symbol,
            substring_i: 29 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_34 as *const symbol,
            substring_i: 33 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_36 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_37 as *const symbol,
            substring_i: 35 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_38 as *const symbol,
            substring_i: 37 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_40 as *const symbol,
            substring_i: 39 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_42 as *const symbol,
            substring_i: 41 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_1_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_46 as *const symbol,
            substring_i: 45 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_47 as *const symbol,
            substring_i: 46 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_50 as *const symbol,
            substring_i: 49 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_1_51 as *const symbol,
            substring_i: 50 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_53 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_55 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_56 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_57 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_1_58 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_1_59 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_1_61 as *const symbol,
            substring_i: 60 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 5] = [
    'o' as i32 as symbol,
    'j' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_1: [symbol; 6] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_2: [symbol; 5] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_3: [symbol; 5] = [
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_4: [symbol; 4] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_5: [symbol; 4] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    'e' as i32 as symbol,
];
static mut s_2_6: [symbol; 5] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_2_7: [symbol; 7] = [
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_2_8: [symbol; 6] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_2_9: [symbol; 6] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    's' as i32 as symbol,
];
static mut s_2_10: [symbol; 4] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'i' as i32 as symbol,
    'u' as i32 as symbol,
];
static mut a_2: [among; 11] = unsafe {
    [
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
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
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 5 as ::core::ffi::c_int,
            s: &raw const s_2_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 7 as ::core::ffi::c_int,
            s: &raw const s_2_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_2_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_2_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 2] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_3_1: [symbol; 3] = [
    'd' as i32 as symbol,
    0xc5 as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
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
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_3_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 2] = ['g' as i32 as symbol, 'd' as i32 as symbol];
static mut a_4: [among; 1] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_4_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut g_v: [::core::ffi::c_uchar; 35] = [
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
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    16 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    64 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    1 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    64 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    0 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    4 as ::core::ffi::c_int as ::core::ffi::c_uchar,
    4 as ::core::ffi::c_int as ::core::ffi::c_uchar,
];
static mut s_0: [symbol; 5] = [
    'a' as i32 as symbol,
    'i' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
];
static mut s_1: [symbol; 5] = [
    'u' as i32 as symbol,
    'o' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
];
static mut s_2: [symbol; 7] = [
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
    'j' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_3: [symbol; 4] = [
    'e' as i32 as symbol,
    's' as i32 as symbol,
    'y' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_4: [symbol; 4] = [
    'a' as i32 as symbol,
    's' as i32 as symbol,
    'y' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_5: [symbol; 6] = [
    'a' as i32 as symbol,
    'v' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_6: [symbol; 6] = [
    'o' as i32 as symbol,
    'j' as i32 as symbol,
    'i' as i32 as symbol,
    'm' as i32 as symbol,
    'a' as i32 as symbol,
    's' as i32 as symbol,
];
static mut s_7: [symbol; 6] = [
    'o' as i32 as symbol,
    'k' as i32 as symbol,
    'a' as i32 as symbol,
    't' as i32 as symbol,
    0xc4 as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
];
static mut s_8: [symbol; 1] = ['t' as i32 as symbol];
static mut s_9: [symbol; 1] = ['d' as i32 as symbol];
static mut s_10: [symbol; 1] = ['g' as i32 as symbol];
unsafe fn r_step1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut mlimit1: ::core::ffi::c_int = 0;
    if (*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize) {
        return 0 as ::core::ffi::c_int;
    }
    mlimit1 = (*z).lb;
    (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_0 as *const among, 204 as ::core::ffi::c_int) == 0 {
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
unsafe fn r_step2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    loop {
        let mut m1: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut mlimit2: ::core::ffi::c_int = 0;
        if !((*z).c < *(*z).I.offset(0 as ::core::ffi::c_int as isize)) {
            mlimit2 = (*z).lb;
            (*z).lb = *(*z).I.offset(0 as ::core::ffi::c_int as isize);
            (*z).ket = (*z).c;
            if find_among_b(z, &raw const a_1 as *const among, 62 as ::core::ffi::c_int)
                == 0
            {
                (*z).lb = mlimit2;
            } else {
                (*z).bra = (*z).c;
                (*z).lb = mlimit2;
                let mut ret: ::core::ffi::c_int = slice_del(z);
                if ret < 0 as ::core::ffi::c_int {
                    return ret;
                }
                continue;
            }
        }
        (*z).c = (*z).l - m1;
        break;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_fix_conflicts(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 3 as ::core::ffi::c_int
        || 2621472 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_2 as *const among,
        11 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_0 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                5 as ::core::ffi::c_int,
                &raw const s_1 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                7 as ::core::ffi::c_int,
                &raw const s_2 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_3 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        5 => {
            let mut ret_3: ::core::ffi::c_int = slice_from_s(
                z,
                4 as ::core::ffi::c_int,
                &raw const s_4 as *const symbol,
            );
            if ret_3 < 0 as ::core::ffi::c_int {
                return ret_3;
            }
        }
        6 => {
            let mut ret_4: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_5 as *const symbol,
            );
            if ret_4 < 0 as ::core::ffi::c_int {
                return ret_4;
            }
        }
        7 => {
            let mut ret_5: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_6 as *const symbol,
            );
            if ret_5 < 0 as ::core::ffi::c_int {
                return ret_5;
            }
        }
        8 => {
            let mut ret_6: ::core::ffi::c_int = slice_from_s(
                z,
                6 as ::core::ffi::c_int,
                &raw const s_7 as *const symbol,
            );
            if ret_6 < 0 as ::core::ffi::c_int {
                return ret_6;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_fix_chdz(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 141 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 190 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(z, &raw const a_3 as *const among, 2 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_8 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                1 as ::core::ffi::c_int,
                &raw const s_9 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_fix_gd(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 100 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_4 as *const among, 1 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_from_s(
        z,
        1 as ::core::ffi::c_int,
        &raw const s_10 as *const symbol,
    );
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn lithuanian_UTF_8_stem(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).l;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut c2: ::core::ffi::c_int = (*z).c;
    let mut c_test3: ::core::ffi::c_int = (*z).c;
    if (*z).c == (*z).l
        || *(*z).p.offset((*z).c as isize) as ::core::ffi::c_int != 'a' as i32
    {
        (*z).c = c2;
    } else {
        (*z).c += 1;
        (*z).c = c_test3;
        if len_utf8((*z).p) <= 6 as ::core::ffi::c_int {
            (*z).c = c2;
        } else {
            let mut ret: ::core::ffi::c_int = skip_utf8(
                (*z).p,
                (*z).c,
                (*z).l,
                1 as ::core::ffi::c_int,
            );
            if ret < 0 as ::core::ffi::c_int {
                (*z).c = c2;
            } else {
                (*z).c = ret;
            }
        }
    }
    let mut ret_0: ::core::ffi::c_int = out_grouping_U(
        z,
        &raw const g_v as *const ::core::ffi::c_uchar,
        97 as ::core::ffi::c_int,
        371 as ::core::ffi::c_int,
        1 as ::core::ffi::c_int,
    );
    if !(ret_0 < 0 as ::core::ffi::c_int) {
        (*z).c += ret_0;
        let mut ret_1: ::core::ffi::c_int = in_grouping_U(
            z,
            &raw const g_v as *const ::core::ffi::c_uchar,
            97 as ::core::ffi::c_int,
            371 as ::core::ffi::c_int,
            1 as ::core::ffi::c_int,
        );
        if !(ret_1 < 0 as ::core::ffi::c_int) {
            (*z).c += ret_1;
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = (*z).c;
        }
    }
    (*z).c = c1;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_2: ::core::ffi::c_int = r_fix_conflicts(z);
    if ret_2 < 0 as ::core::ffi::c_int {
        return ret_2;
    }
    (*z).c = (*z).l - m4;
    let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_3: ::core::ffi::c_int = r_step1(z);
    if ret_3 < 0 as ::core::ffi::c_int {
        return ret_3;
    }
    (*z).c = (*z).l - m5;
    let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_4: ::core::ffi::c_int = r_fix_chdz(z);
    if ret_4 < 0 as ::core::ffi::c_int {
        return ret_4;
    }
    (*z).c = (*z).l - m6;
    let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_5: ::core::ffi::c_int = r_step2(z);
    if ret_5 < 0 as ::core::ffi::c_int {
        return ret_5;
    }
    (*z).c = (*z).l - m7;
    let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_6: ::core::ffi::c_int = r_fix_chdz(z);
    if ret_6 < 0 as ::core::ffi::c_int {
        return ret_6;
    }
    (*z).c = (*z).l - m8;
    let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut ret_7: ::core::ffi::c_int = r_fix_gd(z);
    if ret_7 < 0 as ::core::ffi::c_int {
        return ret_7;
    }
    (*z).c = (*z).l - m9;
    (*z).c = (*z).lb;
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn lithuanian_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 1 as ::core::ffi::c_int);
}
pub unsafe fn lithuanian_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
