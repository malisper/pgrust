//! Idiomatic port of PostgreSQL's `src/common/unicode_category.c`.
//!
//! Determine the general category and character properties of Unicode
//! characters. The encoding is assumed to be UTF-8, where a [`pg_wchar`]
//! is a Unicode code point.
//!
//! Behavior is faithful to PostgreSQL 18.3
//! (`src/common/unicode_category.c` and `unicode_category_table.h`); the
//! generated data tables live in [`mod@tables`].

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

// Internal numeric-width aliases. The generated `tables.rs` data file refers to
// these names (via `use super::*`), and they preserve the exact bit widths used
// by the original C tables. They are not part of the public API.
pub(crate) type uint8 = u8;
pub(crate) type uint32 = u32;

mod tables;

use tables::*;

/// A Unicode code point. Alias of the fabled `PgWChar` (`pg_wchar` in C).
pub type pg_wchar = types_core::PgWChar;

/// Unicode general category.
///
/// Numeric values are chosen to match the corresponding ICU `UCharCategory`,
/// and are guaranteed stable by the Unicode stability policy.
pub type pg_unicode_category = u32;

pub const PG_U_UNASSIGNED: pg_unicode_category = 0; // Cn
pub const PG_U_UPPERCASE_LETTER: pg_unicode_category = 1; // Lu
pub const PG_U_LOWERCASE_LETTER: pg_unicode_category = 2; // Ll
pub const PG_U_TITLECASE_LETTER: pg_unicode_category = 3; // Lt
pub const PG_U_MODIFIER_LETTER: pg_unicode_category = 4; // Lm
pub const PG_U_OTHER_LETTER: pg_unicode_category = 5; // Lo
pub const PG_U_NONSPACING_MARK: pg_unicode_category = 6; // Mn
pub const PG_U_ENCLOSING_MARK: pg_unicode_category = 7; // Me
pub const PG_U_SPACING_MARK: pg_unicode_category = 8; // Mc
pub const PG_U_DECIMAL_NUMBER: pg_unicode_category = 9; // Nd
pub const PG_U_LETTER_NUMBER: pg_unicode_category = 10; // Nl
pub const PG_U_OTHER_NUMBER: pg_unicode_category = 11; // No
pub const PG_U_SPACE_SEPARATOR: pg_unicode_category = 12; // Zs
pub const PG_U_LINE_SEPARATOR: pg_unicode_category = 13; // Zl
pub const PG_U_PARAGRAPH_SEPARATOR: pg_unicode_category = 14; // Zp
pub const PG_U_CONTROL: pg_unicode_category = 15; // Cc
pub const PG_U_FORMAT: pg_unicode_category = 16; // Cf
pub const PG_U_PRIVATE_USE: pg_unicode_category = 17; // Co
pub const PG_U_SURROGATE: pg_unicode_category = 18; // Cs
pub const PG_U_DASH_PUNCTUATION: pg_unicode_category = 19; // Pd
pub const PG_U_OPEN_PUNCTUATION: pg_unicode_category = 20; // Ps
pub const PG_U_CLOSE_PUNCTUATION: pg_unicode_category = 21; // Pe
pub const PG_U_CONNECTOR_PUNCTUATION: pg_unicode_category = 22; // Pc
pub const PG_U_OTHER_PUNCTUATION: pg_unicode_category = 23; // Po
pub const PG_U_MATH_SYMBOL: pg_unicode_category = 24; // Sm
pub const PG_U_CURRENCY_SYMBOL: pg_unicode_category = 25; // Sc
pub const PG_U_MODIFIER_SYMBOL: pg_unicode_category = 26; // Sk
pub const PG_U_OTHER_SYMBOL: pg_unicode_category = 27; // So
pub const PG_U_INITIAL_PUNCTUATION: pg_unicode_category = 28; // Pi
pub const PG_U_FINAL_PUNCTUATION: pg_unicode_category = 29; // Pf

/// A contiguous range of code points sharing one general category.
#[derive(Copy, Clone)]
pub struct pg_category_range {
    pub first: uint32,
    pub last: uint32,
    pub category: uint8,
}

/// General category and compatibility properties for a single code point.
#[derive(Copy, Clone)]
pub struct pg_unicode_properties {
    pub category: uint8,
    pub properties: uint8,
}

/// A contiguous range of code points.
#[derive(Copy, Clone)]
pub struct pg_unicode_range {
    pub first: uint32,
    pub last: uint32,
}

// Compatibility-property bit flags, stored in `pg_unicode_properties.properties`.
// Kept as `c_int`-width values for parity with the C `#define`s; they are masked
// against the `u8` `properties` field after widening.
pub const PG_U_PROP_ALPHABETIC: ::core::ffi::c_int = 1 << 0;
pub const PG_U_PROP_LOWERCASE: ::core::ffi::c_int = 1 << 1;
pub const PG_U_PROP_UPPERCASE: ::core::ffi::c_int = 1 << 2;
pub const PG_U_PROP_CASED: ::core::ffi::c_int = 1 << 3;
pub const PG_U_PROP_CASE_IGNORABLE: ::core::ffi::c_int = 1 << 4;
pub const PG_U_PROP_WHITE_SPACE: ::core::ffi::c_int = 1 << 5;
pub const PG_U_PROP_JOIN_CONTROL: ::core::ffi::c_int = 1 << 6;
pub const PG_U_PROP_HEX_DIGIT: ::core::ffi::c_int = 1 << 7;

const PG_U_CHARACTER_TAB: ::core::ffi::c_int = 0x9;

const CATEGORY_NAMES: [&str; 30] = [
    "Unassigned",
    "Uppercase_Letter",
    "Lowercase_Letter",
    "Titlecase_Letter",
    "Modifier_Letter",
    "Other_Letter",
    "Nonspacing_Mark",
    "Enclosing_Mark",
    "Spacing_Mark",
    "Decimal_Number",
    "Letter_Number",
    "Other_Number",
    "Space_Separator",
    "Line_Separator",
    "Paragraph_Separator",
    "Control",
    "Format",
    "Private_Use",
    "Surrogate",
    "Dash_Punctuation",
    "Open_Punctuation",
    "Close_Punctuation",
    "Connector_Punctuation",
    "Other_Punctuation",
    "Math_Symbol",
    "Currency_Symbol",
    "Modifier_Symbol",
    "Other_Symbol",
    "Initial_Punctuation",
    "Final_Punctuation",
];

const CATEGORY_ABBREVS: [&str; 30] = [
    "Cn", "Lu", "Ll", "Lt", "Lm", "Lo", "Mn", "Me", "Mc", "Nd", "Nl", "No", "Zs", "Zl", "Zp", "Cc",
    "Cf", "Co", "Cs", "Pd", "Ps", "Pe", "Pc", "Po", "Sm", "Sc", "Sk", "So", "Pi", "Pf",
];

/// Unicode general category for the given code point.
pub fn unicode_category(code: pg_wchar) -> pg_unicode_category {
    if code < 0x80 {
        return unicode_opt_ascii[code as usize].category as pg_unicode_category;
    }

    let mut min = 0usize;
    let mut max = unicode_categories.len();
    while min < max {
        let mid = (min + max) / 2;
        let range = unicode_categories[mid];
        if code > range.last {
            min = mid + 1;
        } else if code < range.first {
            max = mid;
        } else {
            return range.category as pg_unicode_category;
        }
    }

    PG_U_UNASSIGNED
}

fn ascii_property(code: pg_wchar, property: ::core::ffi::c_int) -> bool {
    code < 0x80 && unicode_opt_ascii[code as usize].properties as ::core::ffi::c_int & property != 0
}

pub fn pg_u_prop_alphabetic(code: pg_wchar) -> bool {
    ascii_property(code, PG_U_PROP_ALPHABETIC)
        || code >= 0x80 && range_search(&unicode_alphabetic, code)
}

pub fn pg_u_prop_lowercase(code: pg_wchar) -> bool {
    ascii_property(code, PG_U_PROP_LOWERCASE)
        || code >= 0x80 && range_search(&unicode_lowercase, code)
}

pub fn pg_u_prop_uppercase(code: pg_wchar) -> bool {
    ascii_property(code, PG_U_PROP_UPPERCASE)
        || code >= 0x80 && range_search(&unicode_uppercase, code)
}

pub fn pg_u_prop_cased(code: pg_wchar) -> bool {
    if ascii_property(code, PG_U_PROP_CASED) {
        return true;
    }

    category_mask(code) & category_bit(PG_U_TITLECASE_LETTER) != 0
        || pg_u_prop_lowercase(code)
        || pg_u_prop_uppercase(code)
}

pub fn pg_u_prop_case_ignorable(code: pg_wchar) -> bool {
    ascii_property(code, PG_U_PROP_CASE_IGNORABLE)
        || code >= 0x80 && range_search(&unicode_case_ignorable, code)
}

pub fn pg_u_prop_white_space(code: pg_wchar) -> bool {
    ascii_property(code, PG_U_PROP_WHITE_SPACE)
        || code >= 0x80 && range_search(&unicode_white_space, code)
}

pub fn pg_u_prop_hex_digit(code: pg_wchar) -> bool {
    ascii_property(code, PG_U_PROP_HEX_DIGIT)
        || code >= 0x80 && range_search(&unicode_hex_digit, code)
}

pub fn pg_u_prop_join_control(code: pg_wchar) -> bool {
    ascii_property(code, PG_U_PROP_JOIN_CONTROL)
        || code >= 0x80 && range_search(&unicode_join_control, code)
}

pub fn pg_u_isdigit(code: pg_wchar, posix: bool) -> bool {
    if posix {
        b'0' as pg_wchar <= code && code <= b'9' as pg_wchar
    } else {
        unicode_category(code) == PG_U_DECIMAL_NUMBER
    }
}

pub fn pg_u_isalpha(code: pg_wchar) -> bool {
    pg_u_prop_alphabetic(code)
}

pub fn pg_u_isalnum(code: pg_wchar, posix: bool) -> bool {
    pg_u_isalpha(code) || pg_u_isdigit(code, posix)
}

pub fn pg_u_isword(code: pg_wchar) -> bool {
    category_mask(code)
        & (category_bit(PG_U_NONSPACING_MARK)
            | category_bit(PG_U_SPACING_MARK)
            | category_bit(PG_U_ENCLOSING_MARK)
            | category_bit(PG_U_DECIMAL_NUMBER)
            | category_bit(PG_U_CONNECTOR_PUNCTUATION))
        != 0
        || pg_u_isalpha(code)
        || pg_u_prop_join_control(code)
}

pub fn pg_u_isupper(code: pg_wchar) -> bool {
    pg_u_prop_uppercase(code)
}

pub fn pg_u_islower(code: pg_wchar) -> bool {
    pg_u_prop_lowercase(code)
}

pub fn pg_u_isblank(code: pg_wchar) -> bool {
    code == PG_U_CHARACTER_TAB as pg_wchar || unicode_category(code) == PG_U_SPACE_SEPARATOR
}

pub fn pg_u_iscntrl(code: pg_wchar) -> bool {
    unicode_category(code) == PG_U_CONTROL
}

pub fn pg_u_isgraph(code: pg_wchar) -> bool {
    category_mask(code)
        & (category_bit(PG_U_CONTROL)
            | category_bit(PG_U_SURROGATE)
            | category_bit(PG_U_UNASSIGNED))
        == 0
        && !pg_u_isspace(code)
}

pub fn pg_u_isprint(code: pg_wchar) -> bool {
    unicode_category(code) != PG_U_CONTROL && (pg_u_isgraph(code) || pg_u_isblank(code))
}

pub fn pg_u_ispunct(code: pg_wchar, posix: bool) -> bool {
    if posix && pg_u_isalpha(code) {
        return false;
    }

    let mut mask = category_bit(PG_U_CONNECTOR_PUNCTUATION)
        | category_bit(PG_U_DASH_PUNCTUATION)
        | category_bit(PG_U_OPEN_PUNCTUATION)
        | category_bit(PG_U_CLOSE_PUNCTUATION)
        | category_bit(PG_U_INITIAL_PUNCTUATION)
        | category_bit(PG_U_FINAL_PUNCTUATION)
        | category_bit(PG_U_OTHER_PUNCTUATION);
    if posix {
        mask |= category_bit(PG_U_MATH_SYMBOL)
            | category_bit(PG_U_CURRENCY_SYMBOL)
            | category_bit(PG_U_MODIFIER_SYMBOL)
            | category_bit(PG_U_OTHER_SYMBOL);
    }

    category_mask(code) & mask != 0
}

pub fn pg_u_isspace(code: pg_wchar) -> bool {
    pg_u_prop_white_space(code)
}

pub fn pg_u_isxdigit(code: pg_wchar, posix: bool) -> bool {
    if posix {
        (b'0' as pg_wchar <= code && code <= b'9' as pg_wchar)
            || (b'A' as pg_wchar <= code && code <= b'F' as pg_wchar)
            || (b'a' as pg_wchar <= code && code <= b'f' as pg_wchar)
    } else {
        unicode_category(code) == PG_U_DECIMAL_NUMBER || pg_u_prop_hex_digit(code)
    }
}

/// Description of a Unicode general category.
pub fn unicode_category_string(category: pg_unicode_category) -> &'static str {
    CATEGORY_NAMES
        .get(category as usize)
        .copied()
        .unwrap_or("Unrecognized")
}

/// Short code for a Unicode general category.
pub fn unicode_category_abbrev(category: pg_unicode_category) -> &'static str {
    CATEGORY_ABBREVS
        .get(category as usize)
        .copied()
        .unwrap_or("??")
}

/// Install this unit's seams. The `unicode_category` seam is declared
/// `-> i32`; the pure table lookup returns a `pg_unicode_category` (`u32`)
/// which is always a small enum value, widened to `int` exactly as the C
/// caller (`unicode_assigned` in varlena) reads it.
pub fn init_seams() {
    unicode_category_seams::unicode_category::set(|code| unicode_category(code) as i32);
}

fn category_bit(category: pg_unicode_category) -> uint32 {
    1u32 << category
}

fn category_mask(code: pg_wchar) -> uint32 {
    category_bit(unicode_category(code))
}

/// Binary search to test whether `code` exists in one of the ranges in `tbl`.
fn range_search(tbl: &[pg_unicode_range], code: pg_wchar) -> bool {
    let mut min = 0usize;
    let mut max = tbl.len();
    while min < max {
        let mid = (min + max) / 2;
        let range = tbl[mid];
        if code > range.last {
            min = mid + 1;
        } else if code < range.first {
            max = mid;
        } else {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_categories_and_properties_match_expected_values() {
        assert_eq!(unicode_category(b'A' as pg_wchar), PG_U_UPPERCASE_LETTER);
        assert_eq!(unicode_category(b'a' as pg_wchar), PG_U_LOWERCASE_LETTER);
        assert_eq!(unicode_category(b'0' as pg_wchar), PG_U_DECIMAL_NUMBER);
        assert_eq!(unicode_category(b' ' as pg_wchar), PG_U_SPACE_SEPARATOR);
        assert_eq!(unicode_category(0), PG_U_CONTROL);

        assert!(pg_u_isalpha(b'A' as pg_wchar));
        assert!(pg_u_isdigit(b'9' as pg_wchar, true));
        assert!(pg_u_isxdigit(b'f' as pg_wchar, true));
        assert!(pg_u_isspace(b'\n' as pg_wchar));
        assert!(pg_u_ispunct(b'!' as pg_wchar, false));
    }

    #[test]
    fn non_ascii_representative_codepoints_match_expected_values() {
        assert_eq!(unicode_category('é' as pg_wchar), PG_U_LOWERCASE_LETTER);
        assert_eq!(unicode_category('Ω' as pg_wchar), PG_U_UPPERCASE_LETTER);
        assert_eq!(unicode_category('中' as pg_wchar), PG_U_OTHER_LETTER);
        assert_eq!(unicode_category('€' as pg_wchar), PG_U_CURRENCY_SYMBOL);

        assert!(pg_u_isalpha('中' as pg_wchar));
        assert!(pg_u_isupper('Ω' as pg_wchar));
        assert!(pg_u_islower('é' as pg_wchar));
    }

    #[test]
    fn category_names_and_abbreviations_are_stable() {
        assert_eq!(unicode_category_string(PG_U_UNASSIGNED), "Unassigned");
        assert_eq!(unicode_category_abbrev(PG_U_UNASSIGNED), "Cn");
        assert_eq!(
            unicode_category_string(PG_U_LOWERCASE_LETTER),
            "Lowercase_Letter"
        );
        assert_eq!(unicode_category_abbrev(PG_U_LOWERCASE_LETTER), "Ll");
        assert_eq!(unicode_category_string(99), "Unrecognized");
        assert_eq!(unicode_category_abbrev(99), "??");
    }
}
