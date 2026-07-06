//! unicode_category.c, the property/compatibility subset the casemap lane
//! consumes (pg_u_isalnum for titlecase word boundaries, Cased/Case_Ignorable
//! for Final_Sigma). Tables build-time generated from the committed
//! unicode_category_table.h (PG 18.3).

mod tables {
    include!(concat!(env!("OUT_DIR"), "/tables.rs"));
}

use tables::*;

pub const PG_U_UNASSIGNED: u8 = 0;
pub const PG_U_TITLECASE_LETTER: u8 = 3;
pub const PG_U_DECIMAL_NUMBER: u8 = 9;
pub const PG_U_SPACE_SEPARATOR: u8 = 12;
pub const PG_U_CONTROL: u8 = 15;
pub const PG_U_SURROGATE: u8 = 18;
// General categories P* (punctuation) and S* (symbol), per unicode_category.h.
const PG_U_P_CATEGORIES: [u8; 7] = [19, 20, 21, 22, 23, 28, 29];
const PG_U_S_CATEGORIES: [u8; 4] = [24, 25, 26, 27];
const PG_U_CHARACTER_TAB: u32 = 0x09;

const PG_U_PROP_ALPHABETIC: u8 = 1 << 0;
const PG_U_PROP_LOWERCASE: u8 = 1 << 1;
const PG_U_PROP_UPPERCASE: u8 = 1 << 2;
const PG_U_PROP_CASED: u8 = 1 << 3;
const PG_U_PROP_CASE_IGNORABLE: u8 = 1 << 4;
const PG_U_PROP_WHITE_SPACE: u8 = 1 << 5;
const PG_U_PROP_JOIN_CONTROL: u8 = 1 << 6;
const PG_U_PROP_HEX_DIGIT: u8 = 1 << 7;

fn range_search(ranges: &[(u32, u32)], code: u32) -> bool {
    ranges
        .binary_search_by(|&(first, last)| {
            if code < first {
                core::cmp::Ordering::Greater
            } else if code > last {
                core::cmp::Ordering::Less
            } else {
                core::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

pub fn unicode_category(code: u32) -> u8 {
    if code < 0x80 {
        return UNICODE_OPT_ASCII[code as usize].0;
    }
    match UNICODE_CATEGORIES.binary_search_by(|&(first, last, _)| {
        if code < first {
            core::cmp::Ordering::Greater
        } else if code > last {
            core::cmp::Ordering::Less
        } else {
            core::cmp::Ordering::Equal
        }
    }) {
        Ok(i) => UNICODE_CATEGORIES[i].2,
        Err(_) => PG_U_UNASSIGNED,
    }
}

fn ascii_prop(code: u32, prop: u8) -> bool {
    UNICODE_OPT_ASCII[code as usize].1 & prop != 0
}

pub fn pg_u_prop_alphabetic(code: u32) -> bool {
    if code < 0x80 {
        return ascii_prop(code, PG_U_PROP_ALPHABETIC);
    }
    range_search(&UNICODE_ALPHABETIC, code)
}

pub fn pg_u_prop_lowercase(code: u32) -> bool {
    if code < 0x80 {
        return ascii_prop(code, PG_U_PROP_LOWERCASE);
    }
    range_search(&UNICODE_LOWERCASE, code)
}

pub fn pg_u_prop_uppercase(code: u32) -> bool {
    if code < 0x80 {
        return ascii_prop(code, PG_U_PROP_UPPERCASE);
    }
    range_search(&UNICODE_UPPERCASE, code)
}

pub fn pg_u_prop_cased(code: u32) -> bool {
    if code < 0x80 {
        return ascii_prop(code, PG_U_PROP_CASED);
    }
    unicode_category(code) == PG_U_TITLECASE_LETTER
        || pg_u_prop_lowercase(code)
        || pg_u_prop_uppercase(code)
}

pub fn pg_u_prop_case_ignorable(code: u32) -> bool {
    if code < 0x80 {
        return ascii_prop(code, PG_U_PROP_CASE_IGNORABLE);
    }
    range_search(&UNICODE_CASE_IGNORABLE, code)
}

pub fn pg_u_prop_white_space(code: u32) -> bool {
    if code < 0x80 {
        return ascii_prop(code, PG_U_PROP_WHITE_SPACE);
    }
    range_search(&UNICODE_WHITE_SPACE, code)
}

pub fn pg_u_prop_hex_digit(code: u32) -> bool {
    if code < 0x80 {
        return ascii_prop(code, PG_U_PROP_HEX_DIGIT);
    }
    range_search(&UNICODE_HEX_DIGIT, code)
}

pub fn pg_u_prop_join_control(code: u32) -> bool {
    if code < 0x80 {
        return ascii_prop(code, PG_U_PROP_JOIN_CONTROL);
    }
    range_search(&UNICODE_JOIN_CONTROL, code)
}

pub fn pg_u_isdigit(code: u32, posix: bool) -> bool {
    if posix {
        (0x30..=0x39).contains(&code)
    } else {
        unicode_category(code) == PG_U_DECIMAL_NUMBER
    }
}

pub fn pg_u_isalpha(code: u32) -> bool {
    pg_u_prop_alphabetic(code)
}

pub fn pg_u_isalnum(code: u32, posix: bool) -> bool {
    pg_u_isalpha(code) || pg_u_isdigit(code, posix)
}

pub fn pg_u_isupper(code: u32) -> bool {
    pg_u_prop_uppercase(code)
}

pub fn pg_u_islower(code: u32) -> bool {
    pg_u_prop_lowercase(code)
}

pub fn pg_u_isblank(code: u32) -> bool {
    code == PG_U_CHARACTER_TAB || unicode_category(code) == PG_U_SPACE_SEPARATOR
}

pub fn pg_u_isgraph(code: u32) -> bool {
    let cat = unicode_category(code);
    if cat == PG_U_CONTROL || cat == PG_U_SURROGATE || cat == PG_U_UNASSIGNED
        || pg_u_isspace(code)
    {
        return false;
    }
    true
}

pub fn pg_u_isprint(code: u32) -> bool {
    if unicode_category(code) == PG_U_CONTROL {
        return false;
    }
    pg_u_isgraph(code) || pg_u_isblank(code)
}

pub fn pg_u_ispunct(code: u32, posix: bool) -> bool {
    if posix && pg_u_isalpha(code) {
        return false;
    }
    let cat = unicode_category(code);
    let in_p = PG_U_P_CATEGORIES.contains(&cat);
    if posix {
        in_p || PG_U_S_CATEGORIES.contains(&cat)
    } else {
        in_p
    }
}

pub fn pg_u_isspace(code: u32) -> bool {
    pg_u_prop_white_space(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_props() {
        assert!(pg_u_isalnum('a' as u32, true));
        assert!(pg_u_isalnum('7' as u32, true));
        assert!(!pg_u_isalnum(' ' as u32, true));
        assert!(pg_u_prop_cased('A' as u32));
        assert!(!pg_u_prop_cased('1' as u32));
        assert!(pg_u_prop_hex_digit('f' as u32));
        assert!(!pg_u_prop_hex_digit('g' as u32));
    }

    // Spot values verified against the PG 18.3 table data.
    #[test]
    fn non_ascii_props() {
        assert!(pg_u_isalpha(0x0391));
        assert!(pg_u_isalpha(0x0416));
        assert!(!pg_u_isalpha(0x2014));
        assert!(pg_u_isdigit(0x0660, false));
        assert!(!pg_u_isdigit(0x0660, true));
        assert!(pg_u_prop_cased(0x01C5));
        assert!(pg_u_prop_case_ignorable(0x0027));
        assert!(pg_u_prop_case_ignorable(0x0301));
        assert!(!pg_u_prop_case_ignorable(0x0041));
        assert_eq!(unicode_category(0x110000), PG_U_UNASSIGNED);
    }
}
