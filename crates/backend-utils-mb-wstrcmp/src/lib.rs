//! Port of `src/backend/utils/mb/wstrcmp.c`.

use std::ffi::CStr;

use types_wchar::{PgWChar, PgWCharStr};

/// Compares a null-terminated byte string with a null-terminated `pg_wchar`
/// string.
///
/// Returns zero when equal, otherwise the unsigned byte value from `s1` minus
/// the `pg_wchar` value from `s2` at the first mismatch.
pub fn pg_char_and_wchar_strcmp(s1: &CStr, s2: &PgWCharStr<'_>) -> i32 {
    let bytes = s1.to_bytes_with_nul();

    for (byte, wchar) in bytes
        .iter()
        .copied()
        .zip(s2.as_slice_with_nul().iter().copied())
    {
        // The C comparison `(pg_wchar) *s1 == *s2` casts the SIGNED `char` to
        // `pg_wchar`, which sign-extends (e.g. (pg_wchar)(char)0xE9 ==
        // 0xFFFFFFE9). Replicate by widening through i8.
        let signed_byte_as_wchar = (byte as i8) as PgWChar;
        if signed_byte_as_wchar != wchar {
            // The C return value is `*(const unsigned char *) s1 - *(s2 - 1)`,
            // i.e. the UNSIGNED (zero-extended) byte minus the pg_wchar.
            return PgWChar::from(byte).wrapping_sub(wchar) as i32;
        }
        if byte == 0 {
            return 0;
        }
    }

    0
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

    #[test]
    fn returns_zero_for_equal_strings() {
        let s1 = cstr("abc");
        let s2 = PgWCharStr::from_slice(&[b'a' as u32, b'b' as u32, b'c' as u32, 0]).unwrap();

        assert_eq!(pg_char_and_wchar_strcmp(&s1, &s2), 0);
    }

    #[test]
    fn returns_byte_minus_wchar_for_first_mismatch() {
        let s1 = cstr("abc");
        let s2 = PgWCharStr::from_slice(&[b'a' as u32, b'd' as u32, b'c' as u32, 0]).unwrap();

        assert_eq!(pg_char_and_wchar_strcmp(&s1, &s2), -2);
    }

    #[test]
    fn compares_nul_terminator_against_longer_wchar_string() {
        let s1 = cstr("ab");
        let s2 = PgWCharStr::from_slice(&[b'a' as u32, b'b' as u32, b'c' as u32, 0]).unwrap();

        assert_eq!(pg_char_and_wchar_strcmp(&s1, &s2), -(b'c' as i32));
    }

    #[test]
    fn compares_longer_byte_string_against_wchar_nul() {
        let s1 = cstr("abc");
        let s2 = PgWCharStr::from_slice(&[b'a' as u32, b'b' as u32, 0]).unwrap();

        assert_eq!(pg_char_and_wchar_strcmp(&s1, &s2), b'c' as i32);
    }

    #[test]
    fn rejects_non_terminated_wchar_slice() {
        assert_eq!(PgWCharStr::from_slice(&[b'a' as u32]), None);
    }

    #[test]
    fn high_byte_matches_sign_extended_wchar() {
        let s1 = CString::new(vec![0xE9u8]).unwrap();
        // s2 holds the sign-extended value -> C treats them as equal.
        let s2 = PgWCharStr::from_slice(&[0xFFFF_FFE9, 0]).unwrap();
        assert_eq!(pg_char_and_wchar_strcmp(&s1, &s2), 0);
    }

    #[test]
    fn high_byte_mismatch_returns_unsigned_difference() {
        let s1 = CString::new(vec![0xE9u8]).unwrap();
        // Sign-extended byte 0xFFFFFFE9 does not match wchar 0xEA, so the loop
        // exits at the first char and returns the UNSIGNED byte (0xE9 = 233)
        // minus the wchar (0xEA = 234) = -1.
        let s2 = PgWCharStr::from_slice(&[0x0000_00EA, 0]).unwrap();
        assert_eq!(pg_char_and_wchar_strcmp(&s1, &s2), -1);
    }
}
