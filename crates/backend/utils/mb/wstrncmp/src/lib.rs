//! Port of `src/backend/utils/mb/wstrncmp.c`, which provides
//! `strncmp`/`strlen` analogues over PostgreSQL wide characters (`pg_wchar`).

use std::ffi::CStr;

use ::types_wchar::{PgWChar, PgWCharStr};

/// Compares up to `n` PostgreSQL wide characters.
pub fn pg_wchar_strncmp(s1: &PgWCharStr<'_>, s2: &PgWCharStr<'_>, n: usize) -> i32 {
    if n == 0 {
        return 0;
    }

    for (idx, (wchar1, wchar2)) in s1
        .as_slice_with_nul()
        .iter()
        .copied()
        .zip(s2.as_slice_with_nul().iter().copied())
        .enumerate()
    {
        if wchar1 != wchar2 {
            return wchar1.wrapping_sub(wchar2) as i32;
        }
        if wchar1 == 0 || idx + 1 == n {
            return 0;
        }
    }

    0
}

/// Compares up to `n` bytes from a C string with PostgreSQL wide characters.
pub fn pg_char_and_wchar_strncmp(s1: &CStr, s2: &PgWCharStr<'_>, n: usize) -> i32 {
    if n == 0 {
        return 0;
    }

    for (idx, (byte, wchar)) in s1
        .to_bytes_with_nul()
        .iter()
        .copied()
        .zip(s2.as_slice_with_nul().iter().copied())
        .enumerate()
    {
        // Unlike wstrcmp.c, the C here casts through `unsigned char`, so the
        // byte is zero-extended on both the comparison and the return value.
        let byte_as_wchar = PgWChar::from(byte);
        if byte_as_wchar != wchar {
            return byte_as_wchar.wrapping_sub(wchar) as i32;
        }
        if byte == 0 || idx + 1 == n {
            return 0;
        }
    }

    0
}

/// Returns the number of PostgreSQL wide characters before the terminating
/// zero.
pub fn pg_wchar_strlen(value: &PgWCharStr<'_>) -> usize {
    value.len()
}

/// Wires this crate's seams. It declares none, so this is a no-op kept for
/// the uniform `seams-init` startup convention.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn cstr(value: &str) -> CString {
        CString::new(value).unwrap()
    }

    fn wchars(value: &[PgWChar]) -> PgWCharStr<'_> {
        PgWCharStr::from_slice(value).unwrap()
    }

    #[test]
    fn wchar_strncmp_returns_zero_for_equal_prefix() {
        let left = wchars(&[b'a' as u32, b'b' as u32, b'c' as u32, 0]);
        let right = wchars(&[b'a' as u32, b'b' as u32, b'd' as u32, 0]);

        assert_eq!(pg_wchar_strncmp(&left, &right, 2), 0);
    }

    #[test]
    fn wchar_strncmp_returns_difference_for_mismatch_before_limit() {
        let left = wchars(&[b'a' as u32, b'b' as u32, b'c' as u32, 0]);
        let right = wchars(&[b'a' as u32, b'd' as u32, b'c' as u32, 0]);

        assert_eq!(pg_wchar_strncmp(&left, &right, 3), -2);
    }

    #[test]
    fn char_and_wchar_strncmp_respects_limit() {
        let left = cstr("abc");
        let right = wchars(&[b'a' as u32, b'b' as u32, b'd' as u32, 0]);

        assert_eq!(pg_char_and_wchar_strncmp(&left, &right, 2), 0);
    }

    #[test]
    fn char_and_wchar_strncmp_returns_difference_before_limit() {
        let left = cstr("abc");
        let right = wchars(&[b'a' as u32, b'd' as u32, b'c' as u32, 0]);

        assert_eq!(pg_char_and_wchar_strncmp(&left, &right, 3), -2);
    }

    #[test]
    fn strncmp_zero_length_is_equal() {
        let left = wchars(&[b'a' as u32, 0]);
        let right = wchars(&[b'b' as u32, 0]);

        assert_eq!(pg_wchar_strncmp(&left, &right, 0), 0);
    }

    #[test]
    fn wchar_strlen_counts_before_nul() {
        let value = wchars(&[b'a' as u32, b'b' as u32, 0, b'c' as u32]);

        assert_eq!(pg_wchar_strlen(&value), 2);
    }
}
