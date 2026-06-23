use crate::types::{among, symbol, SN_env};
use crate::api::{SN_create_env, SN_close_env};
#[allow(unused_imports)]
use crate::utilities::{
    in_grouping, in_grouping_b, out_grouping, out_grouping_b,
    in_grouping_U, in_grouping_b_U, out_grouping_U, out_grouping_b_U,
    find_among, find_among_b, slice_from_s, slice_del, slice_to,
    eq_s, eq_s_b, eq_v_b, insert_s, len_utf8, skip_utf8, skip_b_utf8,
};

static mut s_0_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_0_1: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_0_2: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_0_3: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_0_4: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_0_5: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_0_6: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x90 as ::core::ffi::c_int as symbol,
];
static mut s_0_7: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x91 as ::core::ffi::c_int as symbol,
];
static mut s_0_8: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x92 as ::core::ffi::c_int as symbol,
];
static mut s_0_9: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_0_10: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_0_11: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_0_12: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_0_13: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_0_14: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_0_15: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
];
static mut s_0_16: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_0_17: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_0_18: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_0_19: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_0_20: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_0_21: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_0_22: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_0_23: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_0_24: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_0_25: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_0_26: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_0_27: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_0_28: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_0_29: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_0_30: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_0_31: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_0_32: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_0_33: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_0_34: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_0_35: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x90 as ::core::ffi::c_int as symbol,
];
static mut s_0_36: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x91 as ::core::ffi::c_int as symbol,
];
static mut s_0_37: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x92 as ::core::ffi::c_int as symbol,
];
static mut s_0_38: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x93 as ::core::ffi::c_int as symbol,
];
static mut s_0_39: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x94 as ::core::ffi::c_int as symbol,
];
static mut s_0_40: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_0_41: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x96 as ::core::ffi::c_int as symbol,
];
static mut s_0_42: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
];
static mut s_0_43: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x98 as ::core::ffi::c_int as symbol,
];
static mut s_0_44: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
];
static mut s_0_45: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_0_46: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_0_47: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x9c as ::core::ffi::c_int as symbol,
];
static mut s_0_48: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x9d as ::core::ffi::c_int as symbol,
];
static mut s_0_49: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x9e as ::core::ffi::c_int as symbol,
];
static mut s_0_50: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_0_51: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_0_52: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_0_53: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_0_54: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_0_55: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_0_56: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_0_57: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
];
static mut s_0_58: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_0_59: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_0_60: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_0_61: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_0_62: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_0_63: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_0_64: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
];
static mut s_0_65: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
];
static mut s_0_66: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_0_67: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_0_68: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_0_69: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_0_70: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_0_71: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_0_72: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_0_73: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_74: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
];
static mut s_0_75: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_0_76: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_0_77: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_0_78: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_0_79: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut s_0_80: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xbd as ::core::ffi::c_int as symbol,
];
static mut s_0_81: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xbe as ::core::ffi::c_int as symbol,
];
static mut s_0_82: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
    0xbf as ::core::ffi::c_int as symbol,
];
static mut s_0_83: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x80 as ::core::ffi::c_int as symbol,
];
static mut s_0_84: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_0_85: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_0_86: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_0_87: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_0_88: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_0_89: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_0_90: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_0_91: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_0_92: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_0_93: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_0_94: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x8b as ::core::ffi::c_int as symbol,
];
static mut s_0_95: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x8c as ::core::ffi::c_int as symbol,
];
static mut s_0_96: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x8d as ::core::ffi::c_int as symbol,
];
static mut s_0_97: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x8e as ::core::ffi::c_int as symbol,
];
static mut s_0_98: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x8f as ::core::ffi::c_int as symbol,
];
static mut s_0_99: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x90 as ::core::ffi::c_int as symbol,
];
static mut s_0_100: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x91 as ::core::ffi::c_int as symbol,
];
static mut s_0_101: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x92 as ::core::ffi::c_int as symbol,
];
static mut s_0_102: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x93 as ::core::ffi::c_int as symbol,
];
static mut s_0_103: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x94 as ::core::ffi::c_int as symbol,
];
static mut s_0_104: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x95 as ::core::ffi::c_int as symbol,
];
static mut s_0_105: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x96 as ::core::ffi::c_int as symbol,
];
static mut s_0_106: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x97 as ::core::ffi::c_int as symbol,
];
static mut s_0_107: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x98 as ::core::ffi::c_int as symbol,
];
static mut s_0_108: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x99 as ::core::ffi::c_int as symbol,
];
static mut s_0_109: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x9a as ::core::ffi::c_int as symbol,
];
static mut s_0_110: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x9b as ::core::ffi::c_int as symbol,
];
static mut s_0_111: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x9c as ::core::ffi::c_int as symbol,
];
static mut s_0_112: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x9d as ::core::ffi::c_int as symbol,
];
static mut s_0_113: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x9e as ::core::ffi::c_int as symbol,
];
static mut s_0_114: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0x9f as ::core::ffi::c_int as symbol,
];
static mut s_0_115: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa0 as ::core::ffi::c_int as symbol,
];
static mut s_0_116: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_0_117: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_0_118: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_0_119: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_0_120: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_0_121: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
];
static mut s_0_122: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_0_123: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_0_124: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_0_125: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_0_126: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_0_127: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_0_128: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
];
static mut s_0_129: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
];
static mut s_0_130: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_0_131: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_0_132: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_0_133: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_0_134: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_0_135: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_0_136: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_0_137: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_0_138: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
];
static mut s_0_139: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_0_140: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_0_141: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_0_142: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
];
static mut s_0_143: [symbol; 3] = [
    0xef as ::core::ffi::c_int as symbol,
    0xbb as ::core::ffi::c_int as symbol,
    0xbc as ::core::ffi::c_int as symbol,
];
static mut a_0: [among; 144] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_12 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 5 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_13 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 6 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_14 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 7 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_15 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 8 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_16 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 9 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_17 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 10 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_0_18 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 11 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_19 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 12 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_20 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_21 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 16 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_22 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_23 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 13 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_24 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_25 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 17 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_26 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_27 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 14 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_28 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_29 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_30 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_31 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 15 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_32 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_33 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 18 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_34 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 19 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_35 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 19 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_36 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 19 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_37 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 19 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_38 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 20 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_39 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 20 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_40 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 21 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_41 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 21 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_42 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 21 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_43 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 21 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_44 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 22 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_45 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 22 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_46 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 22 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_47 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 22 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_48 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 23 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_49 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 23 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_50 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 23 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_51 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 23 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_52 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 24 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_53 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 24 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_54 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 24 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_55 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 24 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_56 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 25 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_57 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 25 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_58 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 25 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_59 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 25 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_60 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 26 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_61 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 26 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_62 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 27 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_63 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 27 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_64 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 28 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_65 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 28 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_66 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 29 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_67 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 29 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_68 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 30 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_69 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 30 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_70 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 30 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_71 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 30 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_72 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 31 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_73 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 31 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_74 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 31 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_75 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 31 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_76 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 32 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_77 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 32 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_78 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 32 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_79 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 32 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_80 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 33 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_81 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 33 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_82 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 33 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_83 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 33 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_84 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 34 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_85 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 34 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_86 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 34 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_87 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 34 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_88 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 35 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_89 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 35 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_90 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 35 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_91 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 35 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_92 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 36 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_93 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 36 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_94 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 36 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_95 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 36 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_96 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 37 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_97 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 37 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_98 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 37 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_99 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 37 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_100 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 38 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_101 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 38 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_102 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 38 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_103 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 38 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_104 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 39 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_105 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 39 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_106 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 39 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_107 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 39 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_108 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 40 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_109 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 40 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_110 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 40 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_111 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 40 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_112 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 41 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_113 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 41 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_114 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 41 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_115 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 41 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_116 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 42 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_117 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 42 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_118 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 42 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_119 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 42 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_120 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 43 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_121 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 43 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_122 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 43 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_123 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 43 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_124 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 44 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_125 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 44 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_126 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 44 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_127 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 44 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_128 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 45 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_129 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 45 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_130 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 46 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_131 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 46 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_132 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 47 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_133 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 47 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_134 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 47 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_135 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 47 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_136 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 51 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_137 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 51 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_138 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 49 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_139 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 49 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_140 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 50 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_141 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 50 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_142 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 48 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 3 as ::core::ffi::c_int,
            s: &raw const s_0_143 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 48 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_1_0: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_1_1: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_1_2: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_1_3: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_1_4: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
];
static mut a_1: [among; 5] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
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
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_1_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_2_0: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_2_1: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_2_2: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_2_3: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_2_4: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
];
static mut a_2: [among; 5] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
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
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_2_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_3_0: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_3_1: [symbol; 6] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_3_2: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_3_3: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut a_3: [among; 4] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
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
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_3_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_4_0: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_4_1: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_4_2: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_4_3: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_4_4: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut a_4: [among; 5] = unsafe {
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
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_4_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_5_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_5_1: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut a_5: [among; 2] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_5_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_6_0: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_6_1: [symbol; 6] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_6_2: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_6_3: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut a_6: [among; 4] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_6_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_6_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_7_0: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_7_1: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_7_2: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_7_3: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut a_7: [among; 4] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_7_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: -(1 as ::core::ffi::c_int),
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_7_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_8_0: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_8_1: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_8_2: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_8_3: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut a_8: [among; 4] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_8_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_9_0: [symbol; 6] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_9_1: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_9_2: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut a_9: [among; 3] = unsafe {
    [
        among {
            s_size: 6 as ::core::ffi::c_int,
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_9_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_10_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_10_1: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_10_2: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_10_3: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_10_4: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_10_5: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_10_6: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_10_7: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_10_8: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_10_9: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut a_10: [among; 10] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_10_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_10_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_10_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
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
    ]
};
static mut s_11_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut a_11: [among; 1] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_11_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_12_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_12_1: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_12_2: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut a_12: [among; 3] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_12_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_12_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_12_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_13_0: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut a_13: [among; 1] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_13_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_14_0: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut a_14: [among; 1] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_14_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_15_0: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut a_15: [among; 1] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_15_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_16_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut a_16: [among; 1] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_16_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_17_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_17_1: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_17_2: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_17_3: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_17_4: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_17_5: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_17_6: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_17_7: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_17_8: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_17_9: [symbol; 6] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_17_10: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_17_11: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut a_17: [among; 12] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_2 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_3 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_4 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_17_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_7 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_8 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_17_9 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_17_11 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_18_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_18_1: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_18_2: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_18_3: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_18_4: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_18_5: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_18_6: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_18_7: [symbol; 6] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_18_8: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_18_9: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_18_10: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut a_18: [among; 11] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_18_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_2 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_3 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 3 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_4 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_18_5 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_18_6 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_18_7 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 4 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_8 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_18_9 as *const symbol,
            substring_i: 6 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_18_10 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_19_0: [symbol; 4] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_19_1: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut a_19: [among; 2] = unsafe {
    [
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_19_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
        among {
            s_size: 4 as ::core::ffi::c_int,
            s: &raw const s_19_1 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_20_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_20_1: [symbol; 6] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
    0xd9 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
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
            s_size: 6 as ::core::ffi::c_int,
            s: &raw const s_20_1 as *const symbol,
            substring_i: 0 as ::core::ffi::c_int,
            result: 2 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_21_0: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut a_21: [among; 1] = unsafe {
    [
        among {
            s_size: 2 as ::core::ffi::c_int,
            s: &raw const s_21_0 as *const symbol,
            substring_i: -(1 as ::core::ffi::c_int),
            result: 1 as ::core::ffi::c_int,
            function: None,
        },
    ]
};
static mut s_0: [symbol; 1] = ['0' as i32 as symbol];
static mut s_1: [symbol; 1] = ['1' as i32 as symbol];
static mut s_2: [symbol; 1] = ['2' as i32 as symbol];
static mut s_3: [symbol; 1] = ['3' as i32 as symbol];
static mut s_4: [symbol; 1] = ['4' as i32 as symbol];
static mut s_5: [symbol; 1] = ['5' as i32 as symbol];
static mut s_6: [symbol; 1] = ['6' as i32 as symbol];
static mut s_7: [symbol; 1] = ['7' as i32 as symbol];
static mut s_8: [symbol; 1] = ['8' as i32 as symbol];
static mut s_9: [symbol; 1] = ['9' as i32 as symbol];
static mut s_10: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_11: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_12: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_13: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa6 as ::core::ffi::c_int as symbol,
];
static mut s_14: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_15: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa4 as ::core::ffi::c_int as symbol,
];
static mut s_16: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_17: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_18: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa9 as ::core::ffi::c_int as symbol,
];
static mut s_19: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_20: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xab as ::core::ffi::c_int as symbol,
];
static mut s_21: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xac as ::core::ffi::c_int as symbol,
];
static mut s_22: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xad as ::core::ffi::c_int as symbol,
];
static mut s_23: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xae as ::core::ffi::c_int as symbol,
];
static mut s_24: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaf as ::core::ffi::c_int as symbol,
];
static mut s_25: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb0 as ::core::ffi::c_int as symbol,
];
static mut s_26: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb1 as ::core::ffi::c_int as symbol,
];
static mut s_27: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb2 as ::core::ffi::c_int as symbol,
];
static mut s_28: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
];
static mut s_29: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb4 as ::core::ffi::c_int as symbol,
];
static mut s_30: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb5 as ::core::ffi::c_int as symbol,
];
static mut s_31: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb6 as ::core::ffi::c_int as symbol,
];
static mut s_32: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb7 as ::core::ffi::c_int as symbol,
];
static mut s_33: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb8 as ::core::ffi::c_int as symbol,
];
static mut s_34: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xb9 as ::core::ffi::c_int as symbol,
];
static mut s_35: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xba as ::core::ffi::c_int as symbol,
];
static mut s_36: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x81 as ::core::ffi::c_int as symbol,
];
static mut s_37: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x82 as ::core::ffi::c_int as symbol,
];
static mut s_38: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_39: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
];
static mut s_40: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x85 as ::core::ffi::c_int as symbol,
];
static mut s_41: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_42: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x87 as ::core::ffi::c_int as symbol,
];
static mut s_43: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_44: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x89 as ::core::ffi::c_int as symbol,
];
static mut s_45: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_46: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_47: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_48: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_49: [symbol; 4] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x84 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_50: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa1 as ::core::ffi::c_int as symbol,
];
static mut s_51: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_52: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x88 as ::core::ffi::c_int as symbol,
];
static mut s_53: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_54: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_55: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa2 as ::core::ffi::c_int as symbol,
];
static mut s_56: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_57: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa5 as ::core::ffi::c_int as symbol,
];
static mut s_58: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
];
static mut s_59: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa8 as ::core::ffi::c_int as symbol,
];
static mut s_60: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x83 as ::core::ffi::c_int as symbol,
];
static mut s_61: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
static mut s_62: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_63: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x86 as ::core::ffi::c_int as symbol,
];
static mut s_64: [symbol; 2] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa3 as ::core::ffi::c_int as symbol,
];
static mut s_65: [symbol; 6] = [
    0xd8 as ::core::ffi::c_int as symbol,
    0xa7 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xb3 as ::core::ffi::c_int as symbol,
    0xd8 as ::core::ffi::c_int as symbol,
    0xaa as ::core::ffi::c_int as symbol,
];
static mut s_66: [symbol; 2] = [
    0xd9 as ::core::ffi::c_int as symbol,
    0x8a as ::core::ffi::c_int as symbol,
];
unsafe fn r_Normalize_pre(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut c1: ::core::ffi::c_int = (*z).c;
    loop {
        let mut c2: ::core::ffi::c_int = (*z).c;
        let mut c3: ::core::ffi::c_int = (*z).c;
        (*z).bra = (*z).c;
        among_var = find_among(
            z,
            &raw const a_0 as *const among,
            144 as ::core::ffi::c_int,
        );
        if among_var == 0 {
            (*z).c = c3;
            let mut ret_50: ::core::ffi::c_int = skip_utf8(
                (*z).p,
                (*z).c,
                (*z).l,
                1 as ::core::ffi::c_int,
            );
            if ret_50 < 0 as ::core::ffi::c_int {
                (*z).c = c2;
                break;
            } else {
                (*z).c = ret_50;
            }
        } else {
            (*z).ket = (*z).c;
            match among_var {
                1 => {
                    let mut ret: ::core::ffi::c_int = slice_del(z);
                    if ret < 0 as ::core::ffi::c_int {
                        return ret;
                    }
                }
                2 => {
                    let mut ret_0: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_0 as *const symbol,
                    );
                    if ret_0 < 0 as ::core::ffi::c_int {
                        return ret_0;
                    }
                }
                3 => {
                    let mut ret_1: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_1 as *const symbol,
                    );
                    if ret_1 < 0 as ::core::ffi::c_int {
                        return ret_1;
                    }
                }
                4 => {
                    let mut ret_2: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_2 as *const symbol,
                    );
                    if ret_2 < 0 as ::core::ffi::c_int {
                        return ret_2;
                    }
                }
                5 => {
                    let mut ret_3: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_3 as *const symbol,
                    );
                    if ret_3 < 0 as ::core::ffi::c_int {
                        return ret_3;
                    }
                }
                6 => {
                    let mut ret_4: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_4 as *const symbol,
                    );
                    if ret_4 < 0 as ::core::ffi::c_int {
                        return ret_4;
                    }
                }
                7 => {
                    let mut ret_5: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_5 as *const symbol,
                    );
                    if ret_5 < 0 as ::core::ffi::c_int {
                        return ret_5;
                    }
                }
                8 => {
                    let mut ret_6: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_6 as *const symbol,
                    );
                    if ret_6 < 0 as ::core::ffi::c_int {
                        return ret_6;
                    }
                }
                9 => {
                    let mut ret_7: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_7 as *const symbol,
                    );
                    if ret_7 < 0 as ::core::ffi::c_int {
                        return ret_7;
                    }
                }
                10 => {
                    let mut ret_8: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_8 as *const symbol,
                    );
                    if ret_8 < 0 as ::core::ffi::c_int {
                        return ret_8;
                    }
                }
                11 => {
                    let mut ret_9: ::core::ffi::c_int = slice_from_s(
                        z,
                        1 as ::core::ffi::c_int,
                        &raw const s_9 as *const symbol,
                    );
                    if ret_9 < 0 as ::core::ffi::c_int {
                        return ret_9;
                    }
                }
                12 => {
                    let mut ret_10: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_10 as *const symbol,
                    );
                    if ret_10 < 0 as ::core::ffi::c_int {
                        return ret_10;
                    }
                }
                13 => {
                    let mut ret_11: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_11 as *const symbol,
                    );
                    if ret_11 < 0 as ::core::ffi::c_int {
                        return ret_11;
                    }
                }
                14 => {
                    let mut ret_12: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_12 as *const symbol,
                    );
                    if ret_12 < 0 as ::core::ffi::c_int {
                        return ret_12;
                    }
                }
                15 => {
                    let mut ret_13: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_13 as *const symbol,
                    );
                    if ret_13 < 0 as ::core::ffi::c_int {
                        return ret_13;
                    }
                }
                16 => {
                    let mut ret_14: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_14 as *const symbol,
                    );
                    if ret_14 < 0 as ::core::ffi::c_int {
                        return ret_14;
                    }
                }
                17 => {
                    let mut ret_15: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_15 as *const symbol,
                    );
                    if ret_15 < 0 as ::core::ffi::c_int {
                        return ret_15;
                    }
                }
                18 => {
                    let mut ret_16: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_16 as *const symbol,
                    );
                    if ret_16 < 0 as ::core::ffi::c_int {
                        return ret_16;
                    }
                }
                19 => {
                    let mut ret_17: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_17 as *const symbol,
                    );
                    if ret_17 < 0 as ::core::ffi::c_int {
                        return ret_17;
                    }
                }
                20 => {
                    let mut ret_18: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_18 as *const symbol,
                    );
                    if ret_18 < 0 as ::core::ffi::c_int {
                        return ret_18;
                    }
                }
                21 => {
                    let mut ret_19: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_19 as *const symbol,
                    );
                    if ret_19 < 0 as ::core::ffi::c_int {
                        return ret_19;
                    }
                }
                22 => {
                    let mut ret_20: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_20 as *const symbol,
                    );
                    if ret_20 < 0 as ::core::ffi::c_int {
                        return ret_20;
                    }
                }
                23 => {
                    let mut ret_21: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_21 as *const symbol,
                    );
                    if ret_21 < 0 as ::core::ffi::c_int {
                        return ret_21;
                    }
                }
                24 => {
                    let mut ret_22: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_22 as *const symbol,
                    );
                    if ret_22 < 0 as ::core::ffi::c_int {
                        return ret_22;
                    }
                }
                25 => {
                    let mut ret_23: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_23 as *const symbol,
                    );
                    if ret_23 < 0 as ::core::ffi::c_int {
                        return ret_23;
                    }
                }
                26 => {
                    let mut ret_24: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_24 as *const symbol,
                    );
                    if ret_24 < 0 as ::core::ffi::c_int {
                        return ret_24;
                    }
                }
                27 => {
                    let mut ret_25: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_25 as *const symbol,
                    );
                    if ret_25 < 0 as ::core::ffi::c_int {
                        return ret_25;
                    }
                }
                28 => {
                    let mut ret_26: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_26 as *const symbol,
                    );
                    if ret_26 < 0 as ::core::ffi::c_int {
                        return ret_26;
                    }
                }
                29 => {
                    let mut ret_27: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_27 as *const symbol,
                    );
                    if ret_27 < 0 as ::core::ffi::c_int {
                        return ret_27;
                    }
                }
                30 => {
                    let mut ret_28: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_28 as *const symbol,
                    );
                    if ret_28 < 0 as ::core::ffi::c_int {
                        return ret_28;
                    }
                }
                31 => {
                    let mut ret_29: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_29 as *const symbol,
                    );
                    if ret_29 < 0 as ::core::ffi::c_int {
                        return ret_29;
                    }
                }
                32 => {
                    let mut ret_30: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_30 as *const symbol,
                    );
                    if ret_30 < 0 as ::core::ffi::c_int {
                        return ret_30;
                    }
                }
                33 => {
                    let mut ret_31: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_31 as *const symbol,
                    );
                    if ret_31 < 0 as ::core::ffi::c_int {
                        return ret_31;
                    }
                }
                34 => {
                    let mut ret_32: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_32 as *const symbol,
                    );
                    if ret_32 < 0 as ::core::ffi::c_int {
                        return ret_32;
                    }
                }
                35 => {
                    let mut ret_33: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_33 as *const symbol,
                    );
                    if ret_33 < 0 as ::core::ffi::c_int {
                        return ret_33;
                    }
                }
                36 => {
                    let mut ret_34: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_34 as *const symbol,
                    );
                    if ret_34 < 0 as ::core::ffi::c_int {
                        return ret_34;
                    }
                }
                37 => {
                    let mut ret_35: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_35 as *const symbol,
                    );
                    if ret_35 < 0 as ::core::ffi::c_int {
                        return ret_35;
                    }
                }
                38 => {
                    let mut ret_36: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_36 as *const symbol,
                    );
                    if ret_36 < 0 as ::core::ffi::c_int {
                        return ret_36;
                    }
                }
                39 => {
                    let mut ret_37: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_37 as *const symbol,
                    );
                    if ret_37 < 0 as ::core::ffi::c_int {
                        return ret_37;
                    }
                }
                40 => {
                    let mut ret_38: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_38 as *const symbol,
                    );
                    if ret_38 < 0 as ::core::ffi::c_int {
                        return ret_38;
                    }
                }
                41 => {
                    let mut ret_39: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_39 as *const symbol,
                    );
                    if ret_39 < 0 as ::core::ffi::c_int {
                        return ret_39;
                    }
                }
                42 => {
                    let mut ret_40: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_40 as *const symbol,
                    );
                    if ret_40 < 0 as ::core::ffi::c_int {
                        return ret_40;
                    }
                }
                43 => {
                    let mut ret_41: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_41 as *const symbol,
                    );
                    if ret_41 < 0 as ::core::ffi::c_int {
                        return ret_41;
                    }
                }
                44 => {
                    let mut ret_42: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_42 as *const symbol,
                    );
                    if ret_42 < 0 as ::core::ffi::c_int {
                        return ret_42;
                    }
                }
                45 => {
                    let mut ret_43: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_43 as *const symbol,
                    );
                    if ret_43 < 0 as ::core::ffi::c_int {
                        return ret_43;
                    }
                }
                46 => {
                    let mut ret_44: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_44 as *const symbol,
                    );
                    if ret_44 < 0 as ::core::ffi::c_int {
                        return ret_44;
                    }
                }
                47 => {
                    let mut ret_45: ::core::ffi::c_int = slice_from_s(
                        z,
                        2 as ::core::ffi::c_int,
                        &raw const s_45 as *const symbol,
                    );
                    if ret_45 < 0 as ::core::ffi::c_int {
                        return ret_45;
                    }
                }
                48 => {
                    let mut ret_46: ::core::ffi::c_int = slice_from_s(
                        z,
                        4 as ::core::ffi::c_int,
                        &raw const s_46 as *const symbol,
                    );
                    if ret_46 < 0 as ::core::ffi::c_int {
                        return ret_46;
                    }
                }
                49 => {
                    let mut ret_47: ::core::ffi::c_int = slice_from_s(
                        z,
                        4 as ::core::ffi::c_int,
                        &raw const s_47 as *const symbol,
                    );
                    if ret_47 < 0 as ::core::ffi::c_int {
                        return ret_47;
                    }
                }
                50 => {
                    let mut ret_48: ::core::ffi::c_int = slice_from_s(
                        z,
                        4 as ::core::ffi::c_int,
                        &raw const s_48 as *const symbol,
                    );
                    if ret_48 < 0 as ::core::ffi::c_int {
                        return ret_48;
                    }
                }
                51 => {
                    let mut ret_49: ::core::ffi::c_int = slice_from_s(
                        z,
                        4 as ::core::ffi::c_int,
                        &raw const s_49 as *const symbol,
                    );
                    if ret_49 < 0 as ::core::ffi::c_int {
                        return ret_49;
                    }
                }
                _ => {}
            }
        }
    }
    (*z).c = c1;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Normalize_post(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    let mut c1: ::core::ffi::c_int = (*z).c;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    (*z).ket = (*z).c;
    if !((*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 5 as ::core::ffi::c_int
        || 124 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0)
    {
        if !(find_among_b(z, &raw const a_1 as *const among, 5 as ::core::ffi::c_int)
            == 0)
        {
            (*z).bra = (*z).c;
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_50 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
            (*z).c = (*z).lb;
        }
    }
    (*z).c = c1;
    let mut c2: ::core::ffi::c_int = (*z).c;
    loop {
        let mut c3: ::core::ffi::c_int = (*z).c;
        let mut c4: ::core::ffi::c_int = (*z).c;
        (*z).bra = (*z).c;
        if !((*z).c + 1 as ::core::ffi::c_int >= (*z).l
            || *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int >> 5 as ::core::ffi::c_int
                != 5 as ::core::ffi::c_int
            || 124 as ::core::ffi::c_int
                >> (*(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                    as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
                & 1 as ::core::ffi::c_int == 0)
        {
            among_var = find_among(
                z,
                &raw const a_2 as *const among,
                5 as ::core::ffi::c_int,
            );
            if !(among_var == 0) {
                (*z).ket = (*z).c;
                match among_var {
                    1 => {
                        let mut ret_0: ::core::ffi::c_int = slice_from_s(
                            z,
                            2 as ::core::ffi::c_int,
                            &raw const s_51 as *const symbol,
                        );
                        if ret_0 < 0 as ::core::ffi::c_int {
                            return ret_0;
                        }
                    }
                    2 => {
                        let mut ret_1: ::core::ffi::c_int = slice_from_s(
                            z,
                            2 as ::core::ffi::c_int,
                            &raw const s_52 as *const symbol,
                        );
                        if ret_1 < 0 as ::core::ffi::c_int {
                            return ret_1;
                        }
                    }
                    3 => {
                        let mut ret_2: ::core::ffi::c_int = slice_from_s(
                            z,
                            2 as ::core::ffi::c_int,
                            &raw const s_53 as *const symbol,
                        );
                        if ret_2 < 0 as ::core::ffi::c_int {
                            return ret_2;
                        }
                    }
                    _ => {}
                }
                continue;
            }
        }
        (*z).c = c4;
        let mut ret_3: ::core::ffi::c_int = skip_utf8(
            (*z).p,
            (*z).c,
            (*z).l,
            1 as ::core::ffi::c_int,
        );
        if ret_3 < 0 as ::core::ffi::c_int {
            (*z).c = c3;
            break;
        } else {
            (*z).c = ret_3;
        }
    }
    (*z).c = c2;
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Checks1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    if (*z).c + 3 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 3 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 132 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c + 3 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 167 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among(z, &raw const a_3 as *const among, 4 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 4 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
        }
        2 => {
            if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
            *(*z).I.offset(1 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
            *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Prefix_Step1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    if (*z).c + 3 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 3 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int >> 5 as ::core::ffi::c_int != 5 as ::core::ffi::c_int
        || 188 as ::core::ffi::c_int
            >> (*(*z).p.offset(((*z).c + 3 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int)
            & 1 as ::core::ffi::c_int == 0
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among(z, &raw const a_4 as *const among, 5 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_54 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_55 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_56 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_57 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Prefix_Step2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).bra = (*z).c;
    if (*z).c + 1 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 129 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 136 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among(z, &raw const a_5 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut c1: ::core::ffi::c_int = (*z).c;
    if eq_s(z, 2 as ::core::ffi::c_int, &raw const s_58 as *const symbol) == 0 {
        (*z).c = c1;
    } else {
        return 0 as ::core::ffi::c_int
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Prefix_Step3a_Noun(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    if (*z).c + 3 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 3 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 132 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c + 3 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 167 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among(z, &raw const a_6 as *const among, 4 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 5 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            if len_utf8((*z).p) <= 4 as ::core::ffi::c_int {
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
unsafe fn r_Prefix_Step3b_Noun(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    if (*z).c + 1 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 168 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c + 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 131 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among(z, &raw const a_7 as *const among, 4 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_59 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            if len_utf8((*z).p) <= 3 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_60 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Prefix_Step3_Verb(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).bra = (*z).c;
    among_var = find_among(z, &raw const a_8 as *const among, 4 as ::core::ffi::c_int);
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 4 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_61 as *const symbol,
            );
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            if len_utf8((*z).p) <= 4 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_0: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_62 as *const symbol,
            );
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            if len_utf8((*z).p) <= 4 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_63 as *const symbol,
            );
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            if len_utf8((*z).p) <= 4 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_2: ::core::ffi::c_int = slice_from_s(
                z,
                2 as ::core::ffi::c_int,
                &raw const s_64 as *const symbol,
            );
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Prefix_Step4_Verb(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).bra = (*z).c;
    if (*z).c + 5 as ::core::ffi::c_int >= (*z).l
        || *(*z).p.offset(((*z).c + 5 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 170 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among(z, &raw const a_9 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).ket = (*z).c;
    if len_utf8((*z).p) <= 4 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut ret: ::core::ffi::c_int = slice_from_s(
        z,
        6 as ::core::ffi::c_int,
        &raw const s_65 as *const symbol,
    );
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Noun_Step1a(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_10 as *const among,
        10 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) < 4 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            if len_utf8((*z).p) < 5 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            if len_utf8((*z).p) < 6 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Noun_Step1b(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 134 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_11 as *const among, 1 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) <= 5 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Noun_Step2a(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, &raw const a_12 as *const among, 3 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) <= 4 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Noun_Step2b(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 170 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_13 as *const among, 1 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 5 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Noun_Step2c1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 170 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_14 as *const among, 1 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 4 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Noun_Step2c2(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 169 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_15 as *const among, 1 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 4 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Noun_Step3(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 138 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_16 as *const among, 1 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 3 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Verb_Step1(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_17 as *const among,
        12 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) < 4 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            if len_utf8((*z).p) < 5 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            if len_utf8((*z).p) < 6 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Verb_Step2a(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    among_var = find_among_b(
        z,
        &raw const a_18 as *const among,
        11 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) < 4 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            if len_utf8((*z).p) < 5 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_0: ::core::ffi::c_int = slice_del(z);
            if ret_0 < 0 as ::core::ffi::c_int {
                return ret_0;
            }
        }
        3 => {
            if len_utf8((*z).p) <= 5 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_1: ::core::ffi::c_int = slice_del(z);
            if ret_1 < 0 as ::core::ffi::c_int {
                return ret_1;
            }
        }
        4 => {
            if len_utf8((*z).p) < 6 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret_2: ::core::ffi::c_int = slice_del(z);
            if ret_2 < 0 as ::core::ffi::c_int {
                return ret_2;
            }
        }
        _ => {}
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Verb_Step2b(mut z: *mut SN_env) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 3 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 133 as ::core::ffi::c_int
            && *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
                as ::core::ffi::c_int != 167 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_19 as *const among, 2 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 5 as ::core::ffi::c_int {
        return 0 as ::core::ffi::c_int;
    }
    let mut ret: ::core::ffi::c_int = slice_del(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn r_Suffix_Verb_Step2c(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut among_var: ::core::ffi::c_int = 0;
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 136 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    among_var = find_among_b(
        z,
        &raw const a_20 as *const among,
        2 as ::core::ffi::c_int,
    );
    if among_var == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) < 4 as ::core::ffi::c_int {
                return 0 as ::core::ffi::c_int;
            }
            let mut ret: ::core::ffi::c_int = slice_del(z);
            if ret < 0 as ::core::ffi::c_int {
                return ret;
            }
        }
        2 => {
            if len_utf8((*z).p) < 6 as ::core::ffi::c_int {
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
unsafe fn r_Suffix_All_alef_maqsura(
    mut z: *mut SN_env,
) -> ::core::ffi::c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 as ::core::ffi::c_int <= (*z).lb
        || *(*z).p.offset(((*z).c - 1 as ::core::ffi::c_int) as isize)
            as ::core::ffi::c_int != 137 as ::core::ffi::c_int
    {
        return 0 as ::core::ffi::c_int;
    }
    if find_among_b(z, &raw const a_21 as *const among, 1 as ::core::ffi::c_int) == 0 {
        return 0 as ::core::ffi::c_int;
    }
    (*z).bra = (*z).c;
    let mut ret: ::core::ffi::c_int = slice_from_s(
        z,
        2 as ::core::ffi::c_int,
        &raw const s_66 as *const symbol,
    );
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn arabic_UTF_8_stem(mut z: *mut SN_env) -> ::core::ffi::c_int {
    let mut current_block: u64;
    *(*z).I.offset(2 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    *(*z).I.offset(1 as ::core::ffi::c_int as isize) = 1 as ::core::ffi::c_int;
    *(*z).I.offset(0 as ::core::ffi::c_int as isize) = 0 as ::core::ffi::c_int;
    let mut c1: ::core::ffi::c_int = (*z).c;
    let mut ret: ::core::ffi::c_int = r_Checks1(z);
    if ret < 0 as ::core::ffi::c_int {
        return ret;
    }
    (*z).c = c1;
    let mut ret_0: ::core::ffi::c_int = r_Normalize_pre(z);
    if ret_0 < 0 as ::core::ffi::c_int {
        return ret_0;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;
    let mut m2: ::core::ffi::c_int = (*z).l - (*z).c;
    let mut m3: ::core::ffi::c_int = (*z).l - (*z).c;
    if *(*z).I.offset(1 as ::core::ffi::c_int as isize) == 0 {
        current_block = 2598805070971620873;
    } else {
        let mut m4: ::core::ffi::c_int = (*z).l - (*z).c;
        let mut i: ::core::ffi::c_int = 1 as ::core::ffi::c_int;
        loop {
            let mut m5: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_1: ::core::ffi::c_int = r_Suffix_Verb_Step1(z);
            if ret_1 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m5;
                break;
            } else {
                if ret_1 < 0 as ::core::ffi::c_int {
                    return ret_1;
                }
                i -= 1;
            }
        }
        if i > 0 as ::core::ffi::c_int {
            current_block = 6009106721275671892;
        } else {
            let mut m6: ::core::ffi::c_int = (*z).l - (*z).c;
            let mut ret_2: ::core::ffi::c_int = r_Suffix_Verb_Step2a(z);
            if ret_2 == 0 as ::core::ffi::c_int {
                (*z).c = (*z).l - m6;
                let mut ret_3: ::core::ffi::c_int = r_Suffix_Verb_Step2c(z);
                if ret_3 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m6;
                    let mut ret_4: ::core::ffi::c_int = skip_b_utf8(
                        (*z).p,
                        (*z).c,
                        (*z).lb,
                        1 as ::core::ffi::c_int,
                    );
                    if ret_4 < 0 as ::core::ffi::c_int {
                        current_block = 6009106721275671892;
                    } else {
                        (*z).c = ret_4;
                        current_block = 4151977364712855205;
                    }
                } else {
                    if ret_3 < 0 as ::core::ffi::c_int {
                        return ret_3;
                    }
                    current_block = 4151977364712855205;
                }
            } else {
                if ret_2 < 0 as ::core::ffi::c_int {
                    return ret_2;
                }
                current_block = 4151977364712855205;
            }
        }
        match current_block {
            4151977364712855205 => {}
            _ => {
                (*z).c = (*z).l - m4;
                let mut ret_5: ::core::ffi::c_int = r_Suffix_Verb_Step2b(z);
                if ret_5 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m4;
                    let mut ret_6: ::core::ffi::c_int = r_Suffix_Verb_Step2a(z);
                    if ret_6 == 0 as ::core::ffi::c_int {
                        current_block = 2598805070971620873;
                    } else {
                        if ret_6 < 0 as ::core::ffi::c_int {
                            return ret_6;
                        }
                        current_block = 4151977364712855205;
                    }
                } else {
                    if ret_5 < 0 as ::core::ffi::c_int {
                        return ret_5;
                    }
                    current_block = 4151977364712855205;
                }
            }
        }
    }
    match current_block {
        2598805070971620873 => {
            (*z).c = (*z).l - m3;
            if *(*z).I.offset(2 as ::core::ffi::c_int as isize) == 0 {
                current_block = 16016489275368243180;
            } else {
                let mut m7: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut m8: ::core::ffi::c_int = (*z).l - (*z).c;
                let mut ret_7: ::core::ffi::c_int = r_Suffix_Noun_Step2c2(z);
                if ret_7 == 0 as ::core::ffi::c_int {
                    (*z).c = (*z).l - m8;
                    if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                        let mut ret_8: ::core::ffi::c_int = r_Suffix_Noun_Step1a(z);
                        if ret_8 == 0 as ::core::ffi::c_int {
                            current_block = 18110158220832310432;
                        } else {
                            if ret_8 < 0 as ::core::ffi::c_int {
                                return ret_8;
                            }
                            let mut m9: ::core::ffi::c_int = (*z).l - (*z).c;
                            let mut ret_9: ::core::ffi::c_int = r_Suffix_Noun_Step2a(z);
                            if ret_9 == 0 as ::core::ffi::c_int {
                                (*z).c = (*z).l - m9;
                                let mut ret_10: ::core::ffi::c_int = r_Suffix_Noun_Step2b(
                                    z,
                                );
                                if ret_10 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m9;
                                    let mut ret_11: ::core::ffi::c_int = r_Suffix_Noun_Step2c1(
                                        z,
                                    );
                                    if ret_11 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m9;
                                        let mut ret_12: ::core::ffi::c_int = skip_b_utf8(
                                            (*z).p,
                                            (*z).c,
                                            (*z).lb,
                                            1 as ::core::ffi::c_int,
                                        );
                                        if ret_12 < 0 as ::core::ffi::c_int {
                                            current_block = 18110158220832310432;
                                        } else {
                                            (*z).c = ret_12;
                                            current_block = 10041771570435381152;
                                        }
                                    } else {
                                        if ret_11 < 0 as ::core::ffi::c_int {
                                            return ret_11;
                                        }
                                        current_block = 10041771570435381152;
                                    }
                                } else {
                                    if ret_10 < 0 as ::core::ffi::c_int {
                                        return ret_10;
                                    }
                                    current_block = 10041771570435381152;
                                }
                            } else {
                                if ret_9 < 0 as ::core::ffi::c_int {
                                    return ret_9;
                                }
                                current_block = 10041771570435381152;
                            }
                        }
                    } else {
                        current_block = 18110158220832310432;
                    }
                    match current_block {
                        10041771570435381152 => {}
                        _ => {
                            (*z).c = (*z).l - m8;
                            let mut ret_13: ::core::ffi::c_int = r_Suffix_Noun_Step1b(z);
                            if ret_13 == 0 as ::core::ffi::c_int {
                                current_block = 12686316977422485193;
                            } else {
                                if ret_13 < 0 as ::core::ffi::c_int {
                                    return ret_13;
                                }
                                let mut m10: ::core::ffi::c_int = (*z).l - (*z).c;
                                let mut ret_14: ::core::ffi::c_int = r_Suffix_Noun_Step2a(
                                    z,
                                );
                                if ret_14 == 0 as ::core::ffi::c_int {
                                    (*z).c = (*z).l - m10;
                                    let mut ret_15: ::core::ffi::c_int = r_Suffix_Noun_Step2b(
                                        z,
                                    );
                                    if ret_15 == 0 as ::core::ffi::c_int {
                                        (*z).c = (*z).l - m10;
                                        let mut ret_16: ::core::ffi::c_int = r_Suffix_Noun_Step2c1(
                                            z,
                                        );
                                        if ret_16 == 0 as ::core::ffi::c_int {
                                            current_block = 12686316977422485193;
                                        } else {
                                            if ret_16 < 0 as ::core::ffi::c_int {
                                                return ret_16;
                                            }
                                            current_block = 10041771570435381152;
                                        }
                                    } else {
                                        if ret_15 < 0 as ::core::ffi::c_int {
                                            return ret_15;
                                        }
                                        current_block = 10041771570435381152;
                                    }
                                } else {
                                    if ret_14 < 0 as ::core::ffi::c_int {
                                        return ret_14;
                                    }
                                    current_block = 10041771570435381152;
                                }
                            }
                            match current_block {
                                10041771570435381152 => {}
                                _ => {
                                    (*z).c = (*z).l - m8;
                                    if *(*z).I.offset(0 as ::core::ffi::c_int as isize) == 0 {
                                        let mut ret_17: ::core::ffi::c_int = r_Suffix_Noun_Step2a(
                                            z,
                                        );
                                        if ret_17 == 0 as ::core::ffi::c_int {
                                            current_block = 10434057210388624566;
                                        } else {
                                            if ret_17 < 0 as ::core::ffi::c_int {
                                                return ret_17;
                                            }
                                            current_block = 10041771570435381152;
                                        }
                                    } else {
                                        current_block = 10434057210388624566;
                                    }
                                    match current_block {
                                        10041771570435381152 => {}
                                        _ => {
                                            (*z).c = (*z).l - m8;
                                            let mut ret_18: ::core::ffi::c_int = r_Suffix_Noun_Step2b(
                                                z,
                                            );
                                            if ret_18 == 0 as ::core::ffi::c_int {
                                                (*z).c = (*z).l - m7;
                                            } else if ret_18 < 0 as ::core::ffi::c_int {
                                                return ret_18
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else if ret_7 < 0 as ::core::ffi::c_int {
                    return ret_7
                }
                let mut ret_19: ::core::ffi::c_int = r_Suffix_Noun_Step3(z);
                if ret_19 == 0 as ::core::ffi::c_int {
                    current_block = 16016489275368243180;
                } else {
                    if ret_19 < 0 as ::core::ffi::c_int {
                        return ret_19;
                    }
                    current_block = 4151977364712855205;
                }
            }
            match current_block {
                4151977364712855205 => {}
                _ => {
                    (*z).c = (*z).l - m3;
                    let mut ret_20: ::core::ffi::c_int = r_Suffix_All_alef_maqsura(z);
                    if !(ret_20 == 0 as ::core::ffi::c_int) {
                        if ret_20 < 0 as ::core::ffi::c_int {
                            return ret_20;
                        }
                    }
                }
            }
        }
        _ => {}
    }
    (*z).c = (*z).l - m2;
    (*z).c = (*z).lb;
    let mut c11: ::core::ffi::c_int = (*z).c;
    let mut c12: ::core::ffi::c_int = (*z).c;
    let mut ret_21: ::core::ffi::c_int = r_Prefix_Step1(z);
    if ret_21 == 0 as ::core::ffi::c_int {
        (*z).c = c12;
    } else if ret_21 < 0 as ::core::ffi::c_int {
        return ret_21
    }
    let mut c13: ::core::ffi::c_int = (*z).c;
    let mut ret_22: ::core::ffi::c_int = r_Prefix_Step2(z);
    if ret_22 == 0 as ::core::ffi::c_int {
        (*z).c = c13;
    } else if ret_22 < 0 as ::core::ffi::c_int {
        return ret_22
    }
    let mut c14: ::core::ffi::c_int = (*z).c;
    let mut ret_23: ::core::ffi::c_int = r_Prefix_Step3a_Noun(z);
    if ret_23 == 0 as ::core::ffi::c_int {
        (*z).c = c14;
        if *(*z).I.offset(2 as ::core::ffi::c_int as isize) == 0 {
            current_block = 7043457542890933913;
        } else {
            let mut ret_24: ::core::ffi::c_int = r_Prefix_Step3b_Noun(z);
            if ret_24 == 0 as ::core::ffi::c_int {
                current_block = 7043457542890933913;
            } else {
                if ret_24 < 0 as ::core::ffi::c_int {
                    return ret_24;
                }
                current_block = 17121930106416121605;
            }
        }
        match current_block {
            17121930106416121605 => {}
            _ => {
                (*z).c = c14;
                if !(*(*z).I.offset(1 as ::core::ffi::c_int as isize) == 0) {
                    let mut c15: ::core::ffi::c_int = (*z).c;
                    let mut ret_25: ::core::ffi::c_int = r_Prefix_Step3_Verb(z);
                    if ret_25 == 0 as ::core::ffi::c_int {
                        (*z).c = c15;
                    } else if ret_25 < 0 as ::core::ffi::c_int {
                        return ret_25
                    }
                    let mut ret_26: ::core::ffi::c_int = r_Prefix_Step4_Verb(z);
                    if !(ret_26 == 0 as ::core::ffi::c_int) {
                        if ret_26 < 0 as ::core::ffi::c_int {
                            return ret_26;
                        }
                    }
                }
            }
        }
    } else if ret_23 < 0 as ::core::ffi::c_int {
        return ret_23
    }
    (*z).c = c11;
    let mut ret_27: ::core::ffi::c_int = r_Normalize_post(z);
    if ret_27 < 0 as ::core::ffi::c_int {
        return ret_27;
    }
    return 1 as ::core::ffi::c_int;
}
pub unsafe fn arabic_UTF_8_create_env() -> *mut SN_env {
    return SN_create_env(0 as ::core::ffi::c_int, 3 as ::core::ffi::c_int);
}
pub unsafe fn arabic_UTF_8_close_env(mut z: *mut SN_env) {
    SN_close_env(z, 0 as ::core::ffi::c_int);
}
