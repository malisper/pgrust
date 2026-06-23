//! KeyWord / KeySuffix tables, ASCII indexes, enums, and the working
//! `FormatNode` / `NUMDesc` structs.
//!
//! Faithful port of the static data and `typedef`s at the top of
//! `src/backend/utils/adt/formatting.c` (PG 18.3).

/// `MAX_MULTIBYTE_CHAR_LEN` (mb/pg_wchar.h).  Width of `FormatNode.character`.
pub const MAX_MULTIBYTE_CHAR_LEN: usize = 4;

// ----------
// Routines flags (formatting.c:113)
// ----------
pub const DCH_FLAG: u32 = 0x1; // DATE-TIME flag
pub const NUM_FLAG: u32 = 0x2; // NUMBER flag
pub const STD_FLAG: u32 = 0x4; // STANDARD flag

// ----------
// KeyWord Index (ascii from position 32 (' ') to 126 (~)) (formatting.c:121)
// ----------
pub const KEYWORD_INDEX_SIZE: usize = (b'~' - b' ') as usize;

/// C: `KeyWord_INDEX_FILTER(_c)` (formatting.c:122).
#[inline]
pub fn keyword_index_filter(c: u8) -> bool {
    !(c <= b' ' || c >= b'~')
}

// ----------
// Maximal length of one node (formatting.c:128)
// ----------
pub const DCH_MAX_ITEM_SIZ: usize = 12; // max localized day name
pub const NUM_MAX_ITEM_SIZ: usize = 8; // roman number (RN has 15 chars)

// ----------
// Node and suffix type codes (formatting.c:175)
// ----------
pub const NODE_TYPE_END: u8 = 1;
pub const NODE_TYPE_ACTION: u8 = 2;
pub const NODE_TYPE_CHAR: u8 = 3;
pub const NODE_TYPE_SEPARATOR: u8 = 4;
pub const NODE_TYPE_SPACE: u8 = 5;

pub const SUFFTYPE_PREFIX: i32 = 1;
pub const SUFFTYPE_POSTFIX: i32 = 2;

pub const CLOCK_24_HOUR: i32 = 0;
pub const CLOCK_12_HOUR: i32 = 1;

// ----------
// FromCharDateMode (formatting.c:151)
// ----------
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
#[repr(i32)]
pub enum FromCharDateMode {
    /// `FROM_CHAR_DATE_NONE` -- value does not affect date mode.
    #[default]
    None = 0,
    Gregorian = 1,
    Isoweek = 2,
}

// ----------
// Full months / abbreviated days (formatting.c:192)
// ----------
pub const MONTHS_FULL: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

pub const DAYS_SHORT: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

/// English abbreviated month names (datetime.c `months[]`).
pub const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// English full day names (datetime.c `days[]`).
pub const DAYS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

// ----------
// AD / BC strings (formatting.c:210)
// ----------
pub const A_D_STR: &str = "A.D.";
pub const A_D_LOWER_STR: &str = "a.d.";
pub const AD_STR: &str = "AD";
pub const AD_LOWER_STR: &str = "ad";

pub const B_C_STR: &str = "B.C.";
pub const B_C_LOWER_STR: &str = "b.c.";
pub const BC_STR: &str = "BC";
pub const BC_LOWER_STR: &str = "bc";

/// AD/BC strings for seq_search (formatting.c:230).  Matches for AD have an even
/// index, BC an odd index.
pub const ADBC_STRINGS: [&str; 4] = [AD_LOWER_STR, BC_LOWER_STR, AD_STR, BC_STR];
pub const ADBC_STRINGS_LONG: [&str; 4] = [A_D_LOWER_STR, B_C_LOWER_STR, A_D_STR, B_C_STR];

// ----------
// AM / PM (formatting.c:237)
// ----------
pub const A_M_STR: &str = "A.M.";
pub const A_M_LOWER_STR: &str = "a.m.";
pub const AM_STR: &str = "AM";
pub const AM_LOWER_STR: &str = "am";

pub const P_M_STR: &str = "P.M.";
pub const P_M_LOWER_STR: &str = "p.m.";
pub const PM_STR: &str = "PM";
pub const PM_LOWER_STR: &str = "pm";

pub const AMPM_STRINGS: [&str; 4] = [AM_LOWER_STR, PM_LOWER_STR, AM_STR, PM_STR];
pub const AMPM_STRINGS_LONG: [&str; 4] = [A_M_LOWER_STR, P_M_LOWER_STR, A_M_STR, P_M_STR];

// ----------
// Months in roman-numeral (reverse order) (formatting.c:266)
// ----------
pub const RM_MONTHS_UPPER: [&str; 12] = [
    "XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I",
];

pub const RM_MONTHS_LOWER: [&str; 12] = [
    "xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i",
];

// ----------
// Roman numerals (formatting.c:276)
// ----------
pub const RM1: [&str; 9] = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX"];
pub const RM10: [&str; 9] = ["X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC"];
pub const RM100: [&str; 9] = ["C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM"];

/// C: `MAX_ROMAN_LEN` (formatting.c:304).  'MMMDCCCLXXXVIII' (3888).
pub const MAX_ROMAN_LEN: usize = 15;

/// C: `IS_VALID_SUB_COMB(curr, next)` (formatting.c:284).
#[inline]
pub fn is_valid_sub_comb(curr: u8, next: u8) -> bool {
    (curr == b'I' && (next == b'V' || next == b'X'))
        || (curr == b'X' && (next == b'L' || next == b'C'))
        || (curr == b'C' && (next == b'D' || next == b'M'))
}

/// C: `ROMAN_VAL(r)` (formatting.c:292).  Returns 0 for non-roman characters.
#[inline]
pub fn roman_val(r: u8) -> i32 {
    match r {
        b'I' => 1,
        b'V' => 5,
        b'X' => 10,
        b'L' => 50,
        b'C' => 100,
        b'D' => 500,
        b'M' => 1000,
        _ => 0,
    }
}

// ----------
// Ordinal postfixes (formatting.c:310)
// ----------
pub const NUM_TH_UPPER: [&str; 4] = ["ST", "ND", "RD", "TH"];
pub const NUM_TH_LOWER: [&str; 4] = ["st", "nd", "rd", "th"];

// ----------
// TH flags (formatting.c:317)
// ----------
pub const TH_UPPER: i32 = 1;
pub const TH_LOWER: i32 = 2;

// ----------
// Flags for NUMBER version (formatting.c:341)
// ----------
pub const NUM_F_DECIMAL: i32 = 1 << 1;
pub const NUM_F_LDECIMAL: i32 = 1 << 2;
pub const NUM_F_ZERO: i32 = 1 << 3;
pub const NUM_F_BLANK: i32 = 1 << 4;
pub const NUM_F_FILLMODE: i32 = 1 << 5;
pub const NUM_F_LSIGN: i32 = 1 << 6;
pub const NUM_F_BRACKET: i32 = 1 << 7;
pub const NUM_F_MINUS: i32 = 1 << 8;
pub const NUM_F_PLUS: i32 = 1 << 9;
pub const NUM_F_ROMAN: i32 = 1 << 10;
pub const NUM_F_MULTI: i32 = 1 << 11;
pub const NUM_F_PLUS_POST: i32 = 1 << 12;
pub const NUM_F_MINUS_POST: i32 = 1 << 13;
pub const NUM_F_EEEE: i32 = 1 << 14;

pub const NUM_LSIGN_PRE: i32 = -1;
pub const NUM_LSIGN_POST: i32 = 1;
pub const NUM_LSIGN_NONE: i32 = 0;

// ----------
// Cache sizing (formatting.c:395).  DCH_CACHE_SIZE / NUM_CACHE_SIZE are
// computed below to match the C macros.  sizeof(FormatNode) in C is 16
// (uint8 + char[5] + uint8 + pointer, MAXALIGNed to 16); the cache size only
// affects when we fall back to the no-cache parser path, so we compute it the
// same way to keep the threshold identical.
// ----------
const FORMAT_NODE_SIZEOF_C: usize = 16; // sizeof(FormatNode) on 64-bit
const NUMDESC_SIZEOF_C: usize = 36; // sizeof(NUMDesc) == 9 * sizeof(int)
const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}
const DCH_CACHE_OVERHEAD: usize =
    maxalign(std::mem::size_of::<bool>() + std::mem::size_of::<i32>());
const NUM_CACHE_OVERHEAD: usize =
    maxalign(std::mem::size_of::<bool>() + std::mem::size_of::<i32>() + NUMDESC_SIZEOF_C);

pub const DCH_CACHE_SIZE: usize = (2048 - DCH_CACHE_OVERHEAD) / (FORMAT_NODE_SIZEOF_C + 1) - 1;
pub const NUM_CACHE_SIZE: usize = (1024 - NUM_CACHE_OVERHEAD) / (FORMAT_NODE_SIZEOF_C + 1) - 1;

pub const DCH_CACHE_ENTRIES: usize = 20;
pub const NUM_CACHE_ENTRIES: usize = 20;

// ----------
// Suffix codes (formatting.c:582)
// ----------
pub const DCH_S_FM: u8 = 0x01;
pub const DCH_S_TH: u8 = 0x02;
pub const DCH_S_TH_LOWER: u8 = 0x04;
pub const DCH_S_SP: u8 = 0x08;
pub const DCH_S_TM: u8 = 0x10;

/// C: `S_THth(_s)` (formatting.c:592).
#[inline]
pub fn s_thth(s: u8) -> bool {
    (s & DCH_S_TH) != 0 || (s & DCH_S_TH_LOWER) != 0
}
/// C: `S_TH(_s)`.
#[inline]
pub fn s_th(s: u8) -> bool {
    (s & DCH_S_TH) != 0
}
/// C: `S_th(_s)`.
#[inline]
pub fn s_th_lower(s: u8) -> bool {
    (s & DCH_S_TH_LOWER) != 0
}
/// C: `S_TH_TYPE(_s)`.
#[inline]
pub fn s_th_type(s: u8) -> i32 {
    if (s & DCH_S_TH) != 0 {
        TH_UPPER
    } else {
        TH_LOWER
    }
}
/// C: `S_FM(_s)`.
#[inline]
pub fn s_fm(s: u8) -> bool {
    (s & DCH_S_FM) != 0
}
/// C: `S_SP(_s)`.
#[inline]
pub fn s_sp(s: u8) -> bool {
    (s & DCH_S_SP) != 0
}
/// C: `S_TM(_s)`.
#[inline]
pub fn s_tm(s: u8) -> bool {
    (s & DCH_S_TM) != 0
}

pub const TM_SUFFIX_LEN: usize = 2;

// ----------
// Return flags for DCH_from_char() (formatting.c:1066)
// ----------
pub const DCH_DATED: i32 = 0x01;
pub const DCH_TIMED: i32 = 0x02;
pub const DCH_ZONED: i32 = 0x04;

/// C: `KeySuffix` (formatting.c:136).
pub struct KeySuffix {
    pub name: &'static str,
    pub len: usize,
    pub id: u8,
    pub typ: i32,
}

/// C: `KeyWord` (formatting.c:158).
pub struct KeyWord {
    pub name: &'static str,
    pub len: usize,
    pub id: i32,
    pub is_digit: bool,
    pub date_mode: FromCharDateMode,
}

/// C: working struct for a parsed format node (formatting.c:167).  The internal
/// representation is idiomatic: `key` is an index into the keyword table rather
/// than a borrowed pointer, since the keyword tables are `'static`.
#[derive(Clone, Debug)]
pub struct FormatNode {
    pub typ: u8,
    pub character: [u8; MAX_MULTIBYTE_CHAR_LEN + 1],
    pub suffix: u8,
    /// Index into the keyword table this node was parsed against (`-1` == none).
    pub key: i32,
}

impl Default for FormatNode {
    fn default() -> Self {
        FormatNode {
            typ: 0,
            character: [0; MAX_MULTIBYTE_CHAR_LEN + 1],
            suffix: 0,
            key: -1,
        }
    }
}

/// C: `NUMDesc` (formatting.c:324).
#[derive(Clone, Debug, Default)]
pub struct NUMDesc {
    pub pre: i32,
    pub post: i32,
    pub lsign: i32,
    pub flag: i32,
    pub pre_lsign_num: i32,
    pub multi: i32,
    pub zero_start: i32,
    pub zero_end: i32,
    pub need_locale: i32,
}

impl NUMDesc {
    /// C: `zeroize_NUM(_n)` (formatting.c:4883).
    pub fn zeroize(&mut self) {
        self.flag = 0;
        self.lsign = 0;
        self.pre = 0;
        self.post = 0;
        self.pre_lsign_num = 0;
        self.need_locale = 0;
        self.multi = 0;
        self.zero_start = 0;
        self.zero_end = 0;
    }
}

// NUMDesc test macros (formatting.c:364).
impl NUMDesc {
    #[inline]
    pub fn is_decimal(&self) -> bool {
        self.flag & NUM_F_DECIMAL != 0
    }
    #[inline]
    pub fn is_ldecimal(&self) -> bool {
        self.flag & NUM_F_LDECIMAL != 0
    }
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.flag & NUM_F_ZERO != 0
    }
    #[inline]
    pub fn is_blank(&self) -> bool {
        self.flag & NUM_F_BLANK != 0
    }
    #[inline]
    pub fn is_fillmode(&self) -> bool {
        self.flag & NUM_F_FILLMODE != 0
    }
    #[inline]
    pub fn is_bracket(&self) -> bool {
        self.flag & NUM_F_BRACKET != 0
    }
    #[inline]
    pub fn is_minus(&self) -> bool {
        self.flag & NUM_F_MINUS != 0
    }
    #[inline]
    pub fn is_lsign(&self) -> bool {
        self.flag & NUM_F_LSIGN != 0
    }
    #[inline]
    pub fn is_plus(&self) -> bool {
        self.flag & NUM_F_PLUS != 0
    }
    #[inline]
    pub fn is_roman(&self) -> bool {
        self.flag & NUM_F_ROMAN != 0
    }
    #[inline]
    pub fn is_multi(&self) -> bool {
        self.flag & NUM_F_MULTI != 0
    }
    #[inline]
    pub fn is_eeee(&self) -> bool {
        self.flag & NUM_F_EEEE != 0
    }
}

// ===========================================================================
// DCH_poz enum (formatting.c:649)
// ===========================================================================
pub const DCH_A_D: i32 = 0;
pub const DCH_A_M: i32 = 1;
pub const DCH_AD: i32 = 2;
pub const DCH_AM: i32 = 3;
pub const DCH_B_C: i32 = 4;
pub const DCH_BC: i32 = 5;
pub const DCH_CC: i32 = 6;
pub const DCH_DAY: i32 = 7;
pub const DCH_DDD: i32 = 8;
pub const DCH_DD: i32 = 9;
pub const DCH_DY: i32 = 10;
pub const DCH_DAY_CAP: i32 = 11; // DCH_Day
pub const DCH_DY_CAP: i32 = 12; // DCH_Dy
pub const DCH_D: i32 = 13;
pub const DCH_FF1: i32 = 14;
pub const DCH_FF2: i32 = 15;
pub const DCH_FF3: i32 = 16;
pub const DCH_FF4: i32 = 17;
pub const DCH_FF5: i32 = 18;
pub const DCH_FF6: i32 = 19;
pub const DCH_FX: i32 = 20;
pub const DCH_HH24: i32 = 21;
pub const DCH_HH12: i32 = 22;
pub const DCH_HH: i32 = 23;
pub const DCH_IDDD: i32 = 24;
pub const DCH_ID: i32 = 25;
pub const DCH_IW: i32 = 26;
pub const DCH_IYYY: i32 = 27;
pub const DCH_IYY: i32 = 28;
pub const DCH_IY: i32 = 29;
pub const DCH_I: i32 = 30;
pub const DCH_J: i32 = 31;
pub const DCH_MI: i32 = 32;
pub const DCH_MM: i32 = 33;
pub const DCH_MONTH: i32 = 34;
pub const DCH_MON: i32 = 35;
pub const DCH_MS: i32 = 36;
pub const DCH_MONTH_CAP: i32 = 37; // DCH_Month
pub const DCH_MON_CAP: i32 = 38; // DCH_Mon
pub const DCH_OF: i32 = 39;
pub const DCH_P_M: i32 = 40;
pub const DCH_PM: i32 = 41;
pub const DCH_Q: i32 = 42;
pub const DCH_RM: i32 = 43;
pub const DCH_SSSSS: i32 = 44;
pub const DCH_SSSS: i32 = 45;
pub const DCH_SS: i32 = 46;
pub const DCH_TZH: i32 = 47;
pub const DCH_TZM: i32 = 48;
pub const DCH_TZ: i32 = 49;
pub const DCH_US: i32 = 50;
pub const DCH_WW: i32 = 51;
pub const DCH_W: i32 = 52;
pub const DCH_Y_YYY: i32 = 53;
pub const DCH_YYYY: i32 = 54;
pub const DCH_YYY: i32 = 55;
pub const DCH_YY: i32 = 56;
pub const DCH_Y: i32 = 57;
pub const DCH_A_D_LOWER: i32 = 58;
pub const DCH_A_M_LOWER: i32 = 59;
pub const DCH_AD_LOWER: i32 = 60;
pub const DCH_AM_LOWER: i32 = 61;
pub const DCH_B_C_LOWER: i32 = 62;
pub const DCH_BC_LOWER: i32 = 63;
pub const DCH_CC_LOWER: i32 = 64;
pub const DCH_DAY_LOWER: i32 = 65;
pub const DCH_DDD_LOWER: i32 = 66;
pub const DCH_DD_LOWER: i32 = 67;
pub const DCH_DY_LOWER: i32 = 68;
pub const DCH_D_LOWER: i32 = 69;
pub const DCH_FF1_LOWER: i32 = 70;
pub const DCH_FF2_LOWER: i32 = 71;
pub const DCH_FF3_LOWER: i32 = 72;
pub const DCH_FF4_LOWER: i32 = 73;
pub const DCH_FF5_LOWER: i32 = 74;
pub const DCH_FF6_LOWER: i32 = 75;
pub const DCH_FX_LOWER: i32 = 76;
pub const DCH_HH24_LOWER: i32 = 77;
pub const DCH_HH12_LOWER: i32 = 78;
pub const DCH_HH_LOWER: i32 = 79;
pub const DCH_IDDD_LOWER: i32 = 80;
pub const DCH_ID_LOWER: i32 = 81;
pub const DCH_IW_LOWER: i32 = 82;
pub const DCH_IYYY_LOWER: i32 = 83;
pub const DCH_IYY_LOWER: i32 = 84;
pub const DCH_IY_LOWER: i32 = 85;
pub const DCH_I_LOWER: i32 = 86;
pub const DCH_J_LOWER: i32 = 87;
pub const DCH_MI_LOWER: i32 = 88;
pub const DCH_MM_LOWER: i32 = 89;
pub const DCH_MONTH_LOWER: i32 = 90; // DCH_month
pub const DCH_MON_LOWER: i32 = 91; // DCH_mon
pub const DCH_MS_LOWER: i32 = 92;
pub const DCH_OF_LOWER: i32 = 93;
pub const DCH_P_M_LOWER: i32 = 94;
pub const DCH_PM_LOWER: i32 = 95;
pub const DCH_Q_LOWER: i32 = 96;
pub const DCH_RM_LOWER: i32 = 97; // DCH_rm
pub const DCH_SSSSS_LOWER: i32 = 98;
pub const DCH_SSSS_LOWER: i32 = 99;
pub const DCH_SS_LOWER: i32 = 100;
pub const DCH_TZH_LOWER: i32 = 101;
pub const DCH_TZM_LOWER: i32 = 102;
pub const DCH_TZ_LOWER: i32 = 103; // DCH_tz
pub const DCH_US_LOWER: i32 = 104;
pub const DCH_WW_LOWER: i32 = 105;
pub const DCH_W_LOWER: i32 = 106;
pub const DCH_Y_YYY_LOWER: i32 = 107;
pub const DCH_YYYY_LOWER: i32 = 108;
pub const DCH_YYY_LOWER: i32 = 109;
pub const DCH_YY_LOWER: i32 = 110;
pub const DCH_Y_LOWER: i32 = 111;

// ===========================================================================
// NUM_poz enum (formatting.c:768)
// ===========================================================================
pub const NUM_COMMA: i32 = 0;
pub const NUM_DEC: i32 = 1;
pub const NUM_0: i32 = 2;
pub const NUM_9: i32 = 3;
pub const NUM_B: i32 = 4;
pub const NUM_C: i32 = 5;
pub const NUM_D: i32 = 6;
pub const NUM_E: i32 = 7;
pub const NUM_FM: i32 = 8;
pub const NUM_G: i32 = 9;
pub const NUM_L: i32 = 10;
pub const NUM_MI: i32 = 11;
pub const NUM_PL: i32 = 12;
pub const NUM_PR: i32 = 13;
pub const NUM_RN: i32 = 14;
pub const NUM_SG: i32 = 15;
pub const NUM_SP: i32 = 16;
pub const NUM_S: i32 = 17;
pub const NUM_TH: i32 = 18;
pub const NUM_V: i32 = 19;
pub const NUM_B_LOWER: i32 = 20;
pub const NUM_C_LOWER: i32 = 21;
pub const NUM_D_LOWER: i32 = 22;
pub const NUM_E_LOWER: i32 = 23;
pub const NUM_FM_LOWER: i32 = 24;
pub const NUM_G_LOWER: i32 = 25;
pub const NUM_L_LOWER: i32 = 26;
pub const NUM_MI_LOWER: i32 = 27;
pub const NUM_PL_LOWER: i32 = 28;
pub const NUM_PR_LOWER: i32 = 29;
pub const NUM_RN_LOWER: i32 = 30; // NUM_rn
pub const NUM_SG_LOWER: i32 = 31;
pub const NUM_SP_LOWER: i32 = 32;
pub const NUM_S_LOWER: i32 = 33;
pub const NUM_TH_LOWER_ID: i32 = 34; // NUM_th
pub const NUM_V_LOWER: i32 = 35;

use FromCharDateMode::{Gregorian as G, Isoweek as ISO, None as N};

/// C: `DCH_suff[]` (formatting.c:608).
pub const DCH_SUFF: &[KeySuffix] = &[
    KeySuffix {
        name: "FM",
        len: 2,
        id: DCH_S_FM,
        typ: SUFFTYPE_PREFIX,
    },
    KeySuffix {
        name: "fm",
        len: 2,
        id: DCH_S_FM,
        typ: SUFFTYPE_PREFIX,
    },
    KeySuffix {
        name: "TM",
        len: TM_SUFFIX_LEN,
        id: DCH_S_TM,
        typ: SUFFTYPE_PREFIX,
    },
    KeySuffix {
        name: "tm",
        len: 2,
        id: DCH_S_TM,
        typ: SUFFTYPE_PREFIX,
    },
    KeySuffix {
        name: "TH",
        len: 2,
        id: DCH_S_TH,
        typ: SUFFTYPE_POSTFIX,
    },
    KeySuffix {
        name: "th",
        len: 2,
        id: DCH_S_TH_LOWER,
        typ: SUFFTYPE_POSTFIX,
    },
    KeySuffix {
        name: "SP",
        len: 2,
        id: DCH_S_SP,
        typ: SUFFTYPE_POSTFIX,
    },
];

macro_rules! kw {
    ($name:expr, $len:expr, $id:expr, $dig:expr, $dm:expr) => {
        KeyWord {
            name: $name,
            len: $len,
            id: $id,
            is_digit: $dig,
            date_mode: $dm,
        }
    };
}

/// C: `DCH_keywords[]` (formatting.c:815).
pub const DCH_KEYWORDS: &[KeyWord] = &[
    kw!("A.D.", 4, DCH_A_D, false, N),
    kw!("A.M.", 4, DCH_A_M, false, N),
    kw!("AD", 2, DCH_AD, false, N),
    kw!("AM", 2, DCH_AM, false, N),
    kw!("B.C.", 4, DCH_B_C, false, N),
    kw!("BC", 2, DCH_BC, false, N),
    kw!("CC", 2, DCH_CC, true, N),
    kw!("DAY", 3, DCH_DAY, false, N),
    kw!("DDD", 3, DCH_DDD, true, G),
    kw!("DD", 2, DCH_DD, true, G),
    kw!("DY", 2, DCH_DY, false, N),
    kw!("Day", 3, DCH_DAY_CAP, false, N),
    kw!("Dy", 2, DCH_DY_CAP, false, N),
    kw!("D", 1, DCH_D, true, G),
    kw!("FF1", 3, DCH_FF1, true, N),
    kw!("FF2", 3, DCH_FF2, true, N),
    kw!("FF3", 3, DCH_FF3, true, N),
    kw!("FF4", 3, DCH_FF4, true, N),
    kw!("FF5", 3, DCH_FF5, true, N),
    kw!("FF6", 3, DCH_FF6, true, N),
    kw!("FX", 2, DCH_FX, false, N),
    kw!("HH24", 4, DCH_HH24, true, N),
    kw!("HH12", 4, DCH_HH12, true, N),
    kw!("HH", 2, DCH_HH, true, N),
    kw!("IDDD", 4, DCH_IDDD, true, ISO),
    kw!("ID", 2, DCH_ID, true, ISO),
    kw!("IW", 2, DCH_IW, true, ISO),
    kw!("IYYY", 4, DCH_IYYY, true, ISO),
    kw!("IYY", 3, DCH_IYY, true, ISO),
    kw!("IY", 2, DCH_IY, true, ISO),
    kw!("I", 1, DCH_I, true, ISO),
    kw!("J", 1, DCH_J, true, N),
    kw!("MI", 2, DCH_MI, true, N),
    kw!("MM", 2, DCH_MM, true, G),
    kw!("MONTH", 5, DCH_MONTH, false, G),
    kw!("MON", 3, DCH_MON, false, G),
    kw!("MS", 2, DCH_MS, true, N),
    kw!("Month", 5, DCH_MONTH_CAP, false, G),
    kw!("Mon", 3, DCH_MON_CAP, false, G),
    kw!("OF", 2, DCH_OF, false, N),
    kw!("P.M.", 4, DCH_P_M, false, N),
    kw!("PM", 2, DCH_PM, false, N),
    kw!("Q", 1, DCH_Q, true, N),
    kw!("RM", 2, DCH_RM, false, G),
    kw!("SSSSS", 5, DCH_SSSS, true, N),
    kw!("SSSS", 4, DCH_SSSS, true, N),
    kw!("SS", 2, DCH_SS, true, N),
    kw!("TZH", 3, DCH_TZH, false, N),
    kw!("TZM", 3, DCH_TZM, true, N),
    kw!("TZ", 2, DCH_TZ, false, N),
    kw!("US", 2, DCH_US, true, N),
    kw!("WW", 2, DCH_WW, true, G),
    kw!("W", 1, DCH_W, true, G),
    kw!("Y,YYY", 5, DCH_Y_YYY, true, G),
    kw!("YYYY", 4, DCH_YYYY, true, G),
    kw!("YYY", 3, DCH_YYY, true, G),
    kw!("YY", 2, DCH_YY, true, G),
    kw!("Y", 1, DCH_Y, true, G),
    kw!("a.d.", 4, DCH_A_D_LOWER, false, N),
    kw!("a.m.", 4, DCH_A_M_LOWER, false, N),
    kw!("ad", 2, DCH_AD_LOWER, false, N),
    kw!("am", 2, DCH_AM_LOWER, false, N),
    kw!("b.c.", 4, DCH_B_C_LOWER, false, N),
    kw!("bc", 2, DCH_BC_LOWER, false, N),
    kw!("cc", 2, DCH_CC, true, N),
    kw!("day", 3, DCH_DAY_LOWER, false, N),
    kw!("ddd", 3, DCH_DDD, true, G),
    kw!("dd", 2, DCH_DD, true, G),
    kw!("dy", 2, DCH_DY_LOWER, false, N),
    kw!("d", 1, DCH_D, true, G),
    kw!("ff1", 3, DCH_FF1, true, N),
    kw!("ff2", 3, DCH_FF2, true, N),
    kw!("ff3", 3, DCH_FF3, true, N),
    kw!("ff4", 3, DCH_FF4, true, N),
    kw!("ff5", 3, DCH_FF5, true, N),
    kw!("ff6", 3, DCH_FF6, true, N),
    kw!("fx", 2, DCH_FX, false, N),
    kw!("hh24", 4, DCH_HH24, true, N),
    kw!("hh12", 4, DCH_HH12, true, N),
    kw!("hh", 2, DCH_HH, true, N),
    kw!("iddd", 4, DCH_IDDD, true, ISO),
    kw!("id", 2, DCH_ID, true, ISO),
    kw!("iw", 2, DCH_IW, true, ISO),
    kw!("iyyy", 4, DCH_IYYY, true, ISO),
    kw!("iyy", 3, DCH_IYY, true, ISO),
    kw!("iy", 2, DCH_IY, true, ISO),
    kw!("i", 1, DCH_I, true, ISO),
    kw!("j", 1, DCH_J, true, N),
    kw!("mi", 2, DCH_MI, true, N),
    kw!("mm", 2, DCH_MM, true, G),
    kw!("month", 5, DCH_MONTH_LOWER, false, G),
    kw!("mon", 3, DCH_MON_LOWER, false, G),
    kw!("ms", 2, DCH_MS, true, N),
    kw!("of", 2, DCH_OF, false, N),
    kw!("p.m.", 4, DCH_P_M_LOWER, false, N),
    kw!("pm", 2, DCH_PM_LOWER, false, N),
    kw!("q", 1, DCH_Q, true, N),
    kw!("rm", 2, DCH_RM_LOWER, false, G),
    kw!("sssss", 5, DCH_SSSS, true, N),
    kw!("ssss", 4, DCH_SSSS, true, N),
    kw!("ss", 2, DCH_SS, true, N),
    kw!("tzh", 3, DCH_TZH, false, N),
    kw!("tzm", 3, DCH_TZM, true, N),
    kw!("tz", 2, DCH_TZ_LOWER, false, N),
    kw!("us", 2, DCH_US, true, N),
    kw!("ww", 2, DCH_WW, true, G),
    kw!("w", 1, DCH_W, true, G),
    kw!("y,yyy", 5, DCH_Y_YYY, true, G),
    kw!("yyyy", 4, DCH_YYYY, true, G),
    kw!("yyy", 3, DCH_YYY, true, G),
    kw!("yy", 2, DCH_YY, true, G),
    kw!("y", 1, DCH_Y, true, G),
];

/// C: `NUM_keywords[]` (formatting.c:940).
pub const NUM_KEYWORDS: &[KeyWord] = &[
    kw!(",", 1, NUM_COMMA, false, N),
    kw!(".", 1, NUM_DEC, false, N),
    kw!("0", 1, NUM_0, false, N),
    kw!("9", 1, NUM_9, false, N),
    kw!("B", 1, NUM_B, false, N),
    kw!("C", 1, NUM_C, false, N),
    kw!("D", 1, NUM_D, false, N),
    kw!("EEEE", 4, NUM_E, false, N),
    kw!("FM", 2, NUM_FM, false, N),
    kw!("G", 1, NUM_G, false, N),
    kw!("L", 1, NUM_L, false, N),
    kw!("MI", 2, NUM_MI, false, N),
    kw!("PL", 2, NUM_PL, false, N),
    kw!("PR", 2, NUM_PR, false, N),
    kw!("RN", 2, NUM_RN, false, N),
    kw!("SG", 2, NUM_SG, false, N),
    kw!("SP", 2, NUM_SP, false, N),
    kw!("S", 1, NUM_S, false, N),
    kw!("TH", 2, NUM_TH, false, N),
    kw!("V", 1, NUM_V, false, N),
    kw!("b", 1, NUM_B, false, N),
    kw!("c", 1, NUM_C, false, N),
    kw!("d", 1, NUM_D, false, N),
    kw!("eeee", 4, NUM_E, false, N),
    kw!("fm", 2, NUM_FM, false, N),
    kw!("g", 1, NUM_G, false, N),
    kw!("l", 1, NUM_L, false, N),
    kw!("mi", 2, NUM_MI, false, N),
    kw!("pl", 2, NUM_PL, false, N),
    kw!("pr", 2, NUM_PR, false, N),
    kw!("rn", 2, NUM_RN_LOWER, false, N),
    kw!("sg", 2, NUM_SG, false, N),
    kw!("sp", 2, NUM_SP, false, N),
    kw!("s", 1, NUM_S, false, N),
    kw!("th", 2, NUM_TH_LOWER_ID, false, N),
    kw!("v", 1, NUM_V, false, N),
];

/// C: `DCH_index[]` (formatting.c:988).
pub const DCH_INDEX: [i32; KEYWORD_INDEX_SIZE] = [
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, //
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, //
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, //
    -1,
    -1,
    -1,
    -1,
    -1,
    DCH_A_D,
    DCH_B_C,
    DCH_CC,
    DCH_DAY,
    -1, //
    DCH_FF1,
    -1,
    DCH_HH24,
    DCH_IDDD,
    DCH_J,
    -1,
    -1,
    DCH_MI,
    -1,
    DCH_OF, //
    DCH_P_M,
    DCH_Q,
    DCH_RM,
    DCH_SSSSS,
    DCH_TZH,
    DCH_US,
    -1,
    DCH_WW,
    -1,
    DCH_Y_YYY, //
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    DCH_A_D_LOWER,
    DCH_B_C_LOWER,
    DCH_CC_LOWER, //
    DCH_DAY_LOWER,
    -1,
    DCH_FF1_LOWER,
    -1,
    DCH_HH24_LOWER,
    DCH_IDDD_LOWER,
    DCH_J_LOWER,
    -1,
    -1,
    DCH_MI_LOWER, //
    -1,
    DCH_OF_LOWER,
    DCH_P_M_LOWER,
    DCH_Q_LOWER,
    DCH_RM_LOWER,
    DCH_SSSSS_LOWER,
    DCH_TZH_LOWER,
    DCH_US_LOWER,
    -1,
    DCH_WW_LOWER, //
    -1,
    DCH_Y_YYY_LOWER,
    -1,
    -1,
    -1,
    -1,
];

/// C: `NUM_index[]` (formatting.c:1012).
pub const NUM_INDEX: [i32; KEYWORD_INDEX_SIZE] = [
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, //
    -1,
    -1,
    -1,
    -1,
    NUM_COMMA,
    -1,
    NUM_DEC,
    -1,
    NUM_0,
    -1, //
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    NUM_9,
    -1,
    -1, //
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    NUM_B,
    NUM_C,
    NUM_D,
    NUM_E, //
    NUM_FM,
    NUM_G,
    -1,
    -1,
    -1,
    -1,
    NUM_L,
    NUM_MI,
    -1,
    -1, //
    NUM_PL,
    -1,
    NUM_RN,
    NUM_SG,
    NUM_TH,
    -1,
    NUM_V,
    -1,
    -1,
    -1, //
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    NUM_B_LOWER,
    NUM_C_LOWER, //
    NUM_D_LOWER,
    NUM_E_LOWER,
    NUM_FM_LOWER,
    NUM_G_LOWER,
    -1,
    -1,
    -1,
    -1,
    NUM_L_LOWER,
    NUM_MI_LOWER, //
    -1,
    -1,
    NUM_PL_LOWER,
    -1,
    NUM_RN_LOWER,
    NUM_SG_LOWER,
    NUM_TH_LOWER_ID,
    -1,
    NUM_V_LOWER,
    -1, //
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_table_sizes_match_c() {
        assert_eq!(DCH_INDEX.len(), KEYWORD_INDEX_SIZE);
        assert_eq!(NUM_INDEX.len(), KEYWORD_INDEX_SIZE);
        assert_eq!(KEYWORD_INDEX_SIZE, 94);
    }

    #[test]
    fn cache_sizes_match_c_macros() {
        // C: (2048 - 8) / (16 + 1) - 1 = 119; (1024 - 48)/17 - 1 = 56.
        assert_eq!(DCH_CACHE_SIZE, 119);
        assert_eq!(NUM_CACHE_SIZE, 56);
    }
}
