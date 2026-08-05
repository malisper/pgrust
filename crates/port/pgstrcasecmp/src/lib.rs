use std::os::raw::c_int;

#[inline]
fn fold(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch + (b'a' - b'A')
    } else if ch >= 0x80 && unsafe { libc::isupper(ch as c_int) } != 0 {
        unsafe { libc::tolower(ch as c_int) as u8 }
    } else {
        ch
    }
}

pub fn pg_strcasecmp(s1: &[u8], s2: &[u8]) -> i32 {
    let n = s1.len().max(s2.len()) + 1;
    for i in 0..n {
        let ch1 = s1.get(i).copied().unwrap_or(0);
        let ch2 = s2.get(i).copied().unwrap_or(0);
        if ch1 != ch2 {
            let f1 = fold(ch1);
            let f2 = fold(ch2);
            if f1 != f2 {
                return f1 as i32 - f2 as i32;
            }
        }
        if ch1 == 0 {
            break;
        }
    }
    0
}

pub fn pg_strncasecmp(s1: &[u8], s2: &[u8], n: usize) -> i32 {
    for i in 0..n {
        let ch1 = s1.get(i).copied().unwrap_or(0);
        let ch2 = s2.get(i).copied().unwrap_or(0);
        if ch1 != ch2 {
            let f1 = fold(ch1);
            let f2 = fold(ch2);
            if f1 != f2 {
                return f1 as i32 - f2 as i32;
            }
        }
        if ch1 == 0 {
            break;
        }
    }
    0
}

pub fn pg_tolower(ch: u8) -> u8 {
    fold(ch)
}

pub fn pg_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - (b'a' - b'A')
    } else if ch >= 0x80 && unsafe { libc::islower(ch as c_int) } != 0 {
        unsafe { libc::toupper(ch as c_int) as u8 }
    } else {
        ch
    }
}

pub fn pg_ascii_tolower(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch + (b'a' - b'A')
    } else {
        ch
    }
}

pub fn pg_ascii_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - (b'a' - b'A')
    } else {
        ch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strcasecmp_ascii_equal() {
        assert_eq!(pg_strcasecmp(b"SELECT", b"select"), 0);
        assert_eq!(pg_strcasecmp(b"Select", b"selecu"), pg_strcasecmp(b"t", b"u"));
    }

    #[test]
    fn strcasecmp_length_mismatch() {
        assert!(pg_strcasecmp(b"abc", b"ab") != 0);
        assert!(pg_strcasecmp(b"ab", b"abc") != 0);
    }

    #[test]
    fn strncasecmp_bounds_at_n() {
        assert_eq!(pg_strncasecmp(b"ABCxyz", b"abcqqq", 3), 0);
        assert_ne!(pg_strncasecmp(b"ABCxyz", b"abcqqq", 4), 0);
    }

    #[test]
    fn strncasecmp_stops_at_nul() {
        assert_eq!(pg_strncasecmp(b"ab\0zz", b"AB\0yy", 5), 0);
    }

    #[test]
    fn tolower_toupper_ascii_roundtrip() {
        for c in b'A'..=b'Z' {
            assert_eq!(pg_tolower(c), c + 32);
        }
        for c in b'a'..=b'z' {
            assert_eq!(pg_toupper(c), c - 32);
        }
        assert_eq!(pg_tolower(b'5'), b'5');
        assert_eq!(pg_toupper(b'5'), b'5');
    }

    #[test]
    fn ascii_variants_ignore_high_bit_locale() {
        assert_eq!(pg_ascii_tolower(b'Z'), b'z');
        assert_eq!(pg_ascii_toupper(b'a'), b'A');
        assert_eq!(pg_ascii_tolower(0xC0), 0xC0);
        assert_eq!(pg_ascii_toupper(0xE0), 0xE0);
    }
}
