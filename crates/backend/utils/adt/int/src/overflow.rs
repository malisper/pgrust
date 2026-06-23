//! The signed-overflow-checking integer primitives from `src/include/common/int.h`
//! (`pg_add_s16_overflow`, ...), ported as plain Rust.  Each returns `true` on
//! overflow and writes the wrapped result through `*result`, matching the C
//! out-parameter convention exactly (Rust's `checked_*` / `overflowing_*` give
//! the same two's-complement wrap as the builtin-based C versions).

/// `pg_add_s16_overflow()` (common/int.h).
#[inline]
pub fn pg_add_s16_overflow(a: i16, b: i16, result: &mut i16) -> bool {
    let (r, o) = a.overflowing_add(b);
    *result = r;
    o
}

/// `pg_sub_s16_overflow()` (common/int.h).
#[inline]
pub fn pg_sub_s16_overflow(a: i16, b: i16, result: &mut i16) -> bool {
    let (r, o) = a.overflowing_sub(b);
    *result = r;
    o
}

/// `pg_mul_s16_overflow()` (common/int.h).
#[inline]
pub fn pg_mul_s16_overflow(a: i16, b: i16, result: &mut i16) -> bool {
    let (r, o) = a.overflowing_mul(b);
    *result = r;
    o
}

/// `pg_add_s32_overflow()` (common/int.h).
#[inline]
pub fn pg_add_s32_overflow(a: i32, b: i32, result: &mut i32) -> bool {
    let (r, o) = a.overflowing_add(b);
    *result = r;
    o
}

/// `pg_sub_s32_overflow()` (common/int.h).
#[inline]
pub fn pg_sub_s32_overflow(a: i32, b: i32, result: &mut i32) -> bool {
    let (r, o) = a.overflowing_sub(b);
    *result = r;
    o
}

/// `pg_mul_s32_overflow()` (common/int.h).
#[inline]
pub fn pg_mul_s32_overflow(a: i32, b: i32, result: &mut i32) -> bool {
    let (r, o) = a.overflowing_mul(b);
    *result = r;
    o
}

/// `pg_add_s64_overflow()` (common/int.h).
#[inline]
pub fn pg_add_s64_overflow(a: i64, b: i64, result: &mut i64) -> bool {
    let (r, o) = a.overflowing_add(b);
    *result = r;
    o
}

/// `pg_sub_s64_overflow()` (common/int.h).
#[inline]
pub fn pg_sub_s64_overflow(a: i64, b: i64, result: &mut i64) -> bool {
    let (r, o) = a.overflowing_sub(b);
    *result = r;
    o
}

/// `pg_mul_s64_overflow()` (common/int.h).
#[inline]
pub fn pg_mul_s64_overflow(a: i64, b: i64, result: &mut i64) -> bool {
    let (r, o) = a.overflowing_mul(b);
    *result = r;
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_overflow_flags_and_wraps() {
        let mut r = 0i32;
        assert!(!pg_add_s32_overflow(1, 2, &mut r));
        assert_eq!(r, 3);
        assert!(pg_add_s32_overflow(i32::MAX, 1, &mut r));
        assert_eq!(r, i32::MIN); // two's-complement wrap, like C
    }

    #[test]
    fn mul_overflow_s16() {
        let mut r = 0i16;
        assert!(pg_mul_s16_overflow(i16::MAX, 2, &mut r));
        assert!(!pg_mul_s16_overflow(100, 3, &mut r));
        assert_eq!(r, 300);
    }
}
