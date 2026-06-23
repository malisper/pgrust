//! `src/port/pgstrcasecmp.c` — portable SQL-like case-independent
//! comparisons and conversions.
//!
//! SQL99 specifies Unicode-aware case normalization, which PostgreSQL does
//! not yet have the infrastructure for; instead `tolower()` provides a
//! locale-aware translation. Some locales get that wrong too (e.g. Turkish
//! `'i'`/`'I'`), so the compromise is `tolower()` only for characters with
//! the high bit set, and ASCII-only downcasing for 7-bit characters.
//!
//! NB: this code should match `downcase_truncate_identifier()` in scansup.c.
//!
//! The C functions take NUL-terminated `const char *`; the owned API takes
//! byte slices and keeps the C-string contract: the walk stops at the first
//! NUL byte, and reading past the end of a slice yields `0` (the implicit
//! terminator), so embedded or trailing NULs behave exactly like C. The
//! high-bit branch defers to the process-global `<ctype.h>` case functions
//! (locale-dependent), exactly as the C does — a direct `libc` call, no seam
//! (the dependency is acyclic).

#![no_std]

/// `IS_HIGHBIT_SET(ch)` (c.h): the byte has its most-significant bit set.
const HIGHBIT: u8 = 0x80;

/// `pg_strcasecmp(s1, s2)` — case-independent comparison of two
/// null-terminated strings.
pub fn pg_strcasecmp(s1: &[u8], s2: &[u8]) -> i32 {
    let mut i = 0usize;
    loop {
        let ch1 = byte_at(s1, i);
        let ch2 = byte_at(s2, i);
        i += 1;

        if ch1 != ch2 {
            let ch1 = fold_to_lower(ch1);
            let ch2 = fold_to_lower(ch2);
            if ch1 != ch2 {
                return i32::from(ch1) - i32::from(ch2);
            }
        }
        if ch1 == 0 {
            break;
        }
    }
    0
}

/// `pg_strncasecmp(s1, s2, n)` — case-independent comparison of two
/// not-necessarily-null-terminated strings; at most `n` bytes are examined
/// from each.
pub fn pg_strncasecmp(s1: &[u8], s2: &[u8], n: usize) -> i32 {
    let mut i = 0usize;
    while i < n {
        let ch1 = byte_at(s1, i);
        let ch2 = byte_at(s2, i);
        i += 1;

        if ch1 != ch2 {
            let ch1 = fold_to_lower(ch1);
            let ch2 = fold_to_lower(ch2);
            if ch1 != ch2 {
                return i32::from(ch1) - i32::from(ch2);
            }
        }
        if ch1 == 0 {
            break;
        }
    }
    0
}

/// `pg_toupper(ch)` — fold a character to upper case.
///
/// Unlike some versions of `toupper()`, this is safe to apply to characters
/// that aren't lower case letters (though the whole thing is a bit bogus for
/// multibyte character sets).
pub fn pg_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - (b'a' - b'A')
    } else if ch & HIGHBIT != 0 && highbit_islower(ch) {
        highbit_toupper(ch)
    } else {
        ch
    }
}

/// `pg_tolower(ch)` — fold a character to lower case.
///
/// Unlike some versions of `tolower()`, this is safe to apply to characters
/// that aren't upper case letters (though the whole thing is a bit bogus for
/// multibyte character sets).
pub fn pg_tolower(ch: u8) -> u8 {
    fold_to_lower(ch)
}

/// `pg_ascii_toupper(ch)` — fold a character to upper case, following
/// C/POSIX locale rules.
pub fn pg_ascii_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - (b'a' - b'A')
    } else {
        ch
    }
}

/// `pg_ascii_tolower(ch)` — fold a character to lower case, following
/// C/POSIX locale rules.
pub fn pg_ascii_tolower(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch + (b'a' - b'A')
    } else {
        ch
    }
}

/// The C-string byte walk: the byte at `i`, or `0` (the implicit terminator)
/// once `i` reaches the end of the slice.
#[inline]
fn byte_at(s: &[u8], i: usize) -> u8 {
    if i < s.len() {
        s[i]
    } else {
        0
    }
}

/// The lower-case fold the comparison loops apply to a differing byte pair —
/// exactly `pg_tolower`'s rule: ASCII `A`-`Z` map down, and a high-bit byte
/// the locale's `isupper` accepts maps via the locale's `tolower`.
#[inline]
fn fold_to_lower(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch + (b'a' - b'A')
    } else if ch & HIGHBIT != 0 && highbit_isupper(ch) {
        highbit_tolower(ch)
    } else {
        ch
    }
}

// The high-bit (>= 0x80) case rules defer to the process locale's `<ctype.h>`
// on a hosted target. On wasm there is no locale facility (C/POSIX only), and
// these ctype symbols are absent from libc; the C/POSIX locale never classifies
// a high-bit byte as alpha, so the faithful single-locale answer is identity.
#[cfg(not(target_family = "wasm"))]
#[inline]
fn highbit_islower(ch: u8) -> bool {
    unsafe { libc::islower(i32::from(ch)) != 0 }
}
#[cfg(not(target_family = "wasm"))]
#[inline]
fn highbit_isupper(ch: u8) -> bool {
    unsafe { libc::isupper(i32::from(ch)) != 0 }
}
#[cfg(not(target_family = "wasm"))]
#[inline]
fn highbit_toupper(ch: u8) -> u8 {
    (unsafe { libc::toupper(i32::from(ch)) }) as u8
}
#[cfg(not(target_family = "wasm"))]
#[inline]
fn highbit_tolower(ch: u8) -> u8 {
    (unsafe { libc::tolower(i32::from(ch)) }) as u8
}

#[cfg(target_family = "wasm")]
#[inline]
fn highbit_islower(_ch: u8) -> bool {
    false
}
#[cfg(target_family = "wasm")]
#[inline]
fn highbit_isupper(_ch: u8) -> bool {
    false
}
#[cfg(target_family = "wasm")]
#[inline]
fn highbit_toupper(ch: u8) -> u8 {
    ch
}
#[cfg(target_family = "wasm")]
#[inline]
fn highbit_tolower(ch: u8) -> u8 {
    ch
}

#[cfg(test)]
mod tests;
