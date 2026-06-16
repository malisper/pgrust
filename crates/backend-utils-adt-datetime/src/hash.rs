//! Hash opclass support for the date/time types (idiomatic, safe Rust).
//!
//! Mirrors the datatype-specific hash functions from
//! `src/backend/utils/adt/date.c` (`hashdate`, `time_hash`, `timetz_hash`)
//! and `src/backend/utils/adt/timestamp.c` (`timestamp_hash`,
//! `timestamptz_hash`, `interval_hash`), together with the small integer-hash
//! folds they delegate to.
//!
//! The hash output is an **on-disk / cross-node / parallel-query contract**:
//! every value here is bit-identical to the C implementation, and must stay so.
//!
//! ## Integer-hash stand-ins (`hash_uint32`, `hashint4`, `hashint8`, ...)
//!
//! `src/backend/access/hash/hashfunc.c` and `src/common/hashfn.c`'s
//! `hash_uint32` / `hash_uint32_extended` are **not yet ported as their own
//! crate**.  The helpers here are minimal faithful copies of the `hashfunc.c`
//! folds, delegating the mixing to the already-ported `common-hashfn` crate
//! (`hash_bytes_uint32` / `hash_bytes_uint32_extended`, the faithful ports of
//! `common/hashfn.c`).  When `access/hash/hashfunc.c` + `utils/hashutils` get a
//! proper home, these stand-ins should be replaced by calls into it; the
//! observable behaviour will not change because the algorithm is identical.
//!
//! Idiomatic surface: plain integers, owned values.  No raw pointers,
//! `extern "C"`, `c_int`, or `pgrust_pg_ffi`.

use common_hashfn::{hash_bytes_uint32, hash_bytes_uint32_extended};

use crate::interval::interval_cmp_value;
use crate::{DateADT, Interval, TimeADT, TimeTzADT, Timestamp, TimestampTz};

// ---------------------------------------------------------------------------
// Step 1: the small integer-hash folds (stand-ins for hashfunc.c + hashutils).
// ---------------------------------------------------------------------------

/// `hash_uint32()` (`common/hashfn.c`) -- hash a 32-bit value.
#[inline]
pub fn hash_uint32(k: u32) -> u32 {
    hash_bytes_uint32(k)
}

/// `hash_uint32_extended()` (`common/hashfn.c`) -- seeded 64-bit variant.
#[inline]
pub fn hash_uint32_extended(k: u32, seed: u64) -> u64 {
    hash_bytes_uint32_extended(k, seed)
}

/// `hashint4()` (`access/hash/hashfunc.c`) -- DATE / int4 hash path.
#[inline]
pub fn hashint4(v: i32) -> u32 {
    hash_uint32(v as u32)
}

/// `hashint4extended()` (`access/hash/hashfunc.c`) -- seeded int4 hash path.
#[inline]
pub fn hashint4extended(v: i32, seed: u64) -> u64 {
    hash_uint32_extended(v as u32, seed)
}

/// `hashint8()` (`access/hash/hashfunc.c`) -- int8 hash path.
///
/// Reproduces the sign-dependent fold EXACTLY: on negative inputs the high half
/// is bitwise-complemented (`~hihalf`) before the XOR.
#[inline]
pub fn hashint8(v: i64) -> u32 {
    let lohalf = v as u32;
    let hihalf = (v >> 32) as u32;
    let lohalf = lohalf ^ if v >= 0 { hihalf } else { !hihalf };
    hash_uint32(lohalf)
}

/// `hashint8extended()` (`access/hash/hashfunc.c`) -- seeded int8 hash path.
#[inline]
pub fn hashint8extended(v: i64, seed: u64) -> u64 {
    let lohalf = v as u32;
    let hihalf = (v >> 32) as u32;
    let lohalf = lohalf ^ if v >= 0 { hihalf } else { !hihalf };
    hash_uint32_extended(lohalf, seed)
}

// ---------------------------------------------------------------------------
// Step 2: the 12 date/time hash functions.
// ---------------------------------------------------------------------------

/// `hashdate()` (`date.c`) -- DATE is an int32, so this is the int4 path.
#[inline]
pub fn hashdate(date: DateADT) -> u32 {
    hashint4(date)
}

/// `hashdateextended()` (`date.c`) -- seeded DATE hash (int4 path).
#[inline]
pub fn hashdateextended(date: DateADT, seed: u64) -> u64 {
    hashint4extended(date, seed)
}

/// `time_hash()` (`date.c`) -- TIME is int64.
#[inline]
pub fn time_hash(time: TimeADT) -> u32 {
    hashint8(time)
}

/// `time_hash_extended()` (`date.c`) -- seeded TIME hash (int8 path).
#[inline]
pub fn time_hash_extended(time: TimeADT, seed: u64) -> u64 {
    hashint8extended(time, seed)
}

/// `timetz_hash()` (`date.c`) -- hash a TIMETZ value: the field hashes are
/// computed separately and XORed (to avoid struct padding issues).
#[inline]
pub fn timetz_hash(key: &TimeTzADT) -> u32 {
    let mut thash = hashint8(key.time);
    thash ^= hash_uint32(key.zone as u32);
    thash
}

/// `timetz_hash_extended()` (`date.c`) -- seeded TIMETZ hash.
#[inline]
pub fn timetz_hash_extended(key: &TimeTzADT, seed: u64) -> u64 {
    let mut thash = hashint8extended(key.time, seed);
    thash ^= hash_uint32_extended(key.zone as u32, seed);
    thash
}

/// `timestamp_hash()` (`timestamp.c`) -- int8 path.
#[inline]
pub fn timestamp_hash(ts: Timestamp) -> u32 {
    hashint8(ts)
}

/// `timestamp_hash_extended()` (`timestamp.c`) -- seeded int8 path.
#[inline]
pub fn timestamp_hash_extended(ts: Timestamp, seed: u64) -> u64 {
    hashint8extended(ts, seed)
}

/// `timestamptz_hash()` (`timestamp.c`) -- int8 path.
#[inline]
pub fn timestamptz_hash(ts: TimestampTz) -> u32 {
    hashint8(ts)
}

/// `timestamptz_hash_extended()` (`timestamp.c`) -- seeded int8 path.
#[inline]
pub fn timestamptz_hash_extended(ts: TimestampTz, seed: u64) -> u64 {
    hashint8extended(ts, seed)
}

/// `interval_hash()` (`timestamp.c`) -- hash an INTERVAL value.
///
/// Produces equal hashvals for values `interval_cmp_internal()` considers equal,
/// by hashing the same net span (`interval_cmp_value`, `i128` here) reduced to
/// its low 64 bits (`int128_to_int64`, i.e. `span as i64`), then [`hashint8`].
#[inline]
pub fn interval_hash(key: &Interval) -> u32 {
    let span = interval_cmp_value(key);
    let span64 = span as i64; // int128_to_int64(span): low 64 bits.
    hashint8(span64)
}

/// `interval_hash_extended()` (`timestamp.c`) -- seeded INTERVAL hash.
#[inline]
pub fn interval_hash_extended(key: &Interval, seed: u64) -> u64 {
    let span = interval_cmp_value(key);
    let span64 = span as i64; // int128_to_int64(span): low 64 bits.
    hashint8extended(span64, seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_hashfn::{hash_bytes_uint32, hash_bytes_uint32_extended};

    fn fold_int8(v: i64) -> u32 {
        let lohalf = v as u32;
        let hihalf = (v >> 32) as u32;
        lohalf ^ if v >= 0 { hihalf } else { !hihalf }
    }

    #[test]
    fn hash_uint32_helpers_delegate_to_common_hashfn() {
        for k in [0u32, 1, 42, 0x0102_0304, u32::MAX] {
            assert_eq!(hash_uint32(k), hash_bytes_uint32(k));
            assert_eq!(hashint4(k as i32), hash_bytes_uint32(k));
            for seed in [0u64, 1, 0x0123_4567_89ab_cdef] {
                assert_eq!(
                    hash_uint32_extended(k, seed),
                    hash_bytes_uint32_extended(k, seed)
                );
                assert_eq!(
                    hashint4extended(k as i32, seed),
                    hash_bytes_uint32_extended(k, seed)
                );
            }
        }
    }

    #[test]
    fn hashint8_reproduces_the_sign_dependent_fold() {
        for v in [
            i64::MIN,
            -1_i64,
            0_i64,
            1_i64,
            i64::MAX,
            -123_456_789_012,
            9_876_543_210,
        ] {
            let folded = fold_int8(v);
            assert_eq!(hashint8(v), hash_bytes_uint32(folded));
            for seed in [0u64, 7, 0x0123_4567_89ab_cdef] {
                assert_eq!(
                    hashint8extended(v, seed),
                    hash_bytes_uint32_extended(folded, seed)
                );
            }
        }
    }

    #[test]
    fn hashint8_negative_branch_uses_bitwise_not_of_hihalf() {
        assert_eq!(fold_int8(-1), 0xFFFF_FFFF);
        assert_eq!(hashint8(-1), hash_bytes_uint32(0xFFFF_FFFF));
        assert_ne!(hashint8(-1), hash_bytes_uint32(0));

        assert_eq!(fold_int8(i64::MIN), 0x7FFF_FFFF);
        assert_eq!(hashint8(i64::MIN), hash_bytes_uint32(0x7FFF_FFFF));

        assert_eq!(fold_int8(i64::MAX), 0x8000_0000);
        assert_eq!(hashint8(i64::MAX), hash_bytes_uint32(0x8000_0000));

        let set = [
            hashint8(i64::MIN),
            hashint8(-1),
            hashint8(0),
            hashint8(i64::MAX),
        ];
        for i in 0..set.len() {
            for j in (i + 1)..set.len() {
                assert_ne!(set[i], set[j], "hashint8 collision at {i},{j}");
            }
        }
    }

    #[test]
    fn hashdate_uses_the_int4_path() {
        for d in [0_i32, 1, -1, 7305, i32::MIN, i32::MAX] {
            let as_u32 = d as u32;
            assert_eq!(hashdate(d), hash_bytes_uint32(as_u32));
            assert_eq!(hashdate(d), hashint4(d));
            for seed in [0u64, 0x0123_4567_89ab_cdef] {
                assert_eq!(
                    hashdateextended(d, seed),
                    hash_bytes_uint32_extended(as_u32, seed)
                );
            }
        }
        let date = 7305_i32;
        let with_high_bits = ((0x1234_5678_u64 << 32) | (date as u32 as u64)) as i64;
        assert_ne!(hashdate(date), hashint8(with_high_bits));
    }

    #[test]
    fn time_hash_uses_int8_path() {
        for t in [
            0_i64,
            1,
            43_200_000_000,
            86_399_999_999,
            86_400_000_000,
        ] {
            assert_eq!(time_hash(t), hashint8(t));
            for seed in [0u64, 0x0123_4567_89ab_cdef] {
                assert_eq!(time_hash_extended(t, seed), hashint8extended(t, seed));
            }
        }
        let v = 43_200_000_000_i64;
        let folded = (v as u32) ^ ((v >> 32) as u32);
        assert_eq!(time_hash(v), hash_bytes_uint32(folded));
    }

    #[test]
    fn timestamp_and_timestamptz_use_int8_path() {
        for ts in [
            0_i64,
            1,
            -1,
            631_152_000_000_000,
            i64::MIN,
            i64::MAX,
        ] {
            assert_eq!(timestamp_hash(ts), hashint8(ts));
            assert_eq!(timestamptz_hash(ts), hashint8(ts));
            assert_eq!(timestamp_hash(ts), timestamptz_hash(ts));
            for seed in [0u64, 0x0123_4567_89ab_cdef] {
                assert_eq!(timestamp_hash_extended(ts, seed), hashint8extended(ts, seed));
                assert_eq!(timestamptz_hash_extended(ts, seed), hashint8extended(ts, seed));
            }
        }
    }

    #[test]
    fn timetz_hash_xors_time_and_zone_field_hashes() {
        let key = TimeTzADT {
            time: 43_200_000_000,
            zone: 0,
        };
        let expected = hashint8(key.time) ^ hash_bytes_uint32(key.zone as u32);
        assert_eq!(timetz_hash(&key), expected);
        for seed in [0u64, 0x0123_4567_89ab_cdef] {
            let exp_e =
                hashint8extended(key.time, seed) ^ hash_bytes_uint32_extended(key.zone as u32, seed);
            assert_eq!(timetz_hash_extended(&key, seed), exp_e);
        }
    }

    #[test]
    fn timetz_equality_invariant() {
        use crate::timetz::timetz_cmp_internal;

        let a = TimeTzADT {
            time: 43_200_000_000,
            zone: 0,
        };
        let b = TimeTzADT {
            time: 43_200_000_000,
            zone: 0,
        };
        assert_eq!(timetz_cmp_internal(&a, &b), 0);
        assert_eq!(timetz_hash(&a), timetz_hash(&b));
        let seed = 0x0123_4567_89ab_cdef;
        assert_eq!(timetz_hash_extended(&a, seed), timetz_hash_extended(&b, seed));

        use types_datetime::USECS_PER_SEC;
        let noon_utc = TimeTzADT {
            time: 43_200_000_000,
            zone: 0,
        };
        let one_pm_plus1 = TimeTzADT {
            time: 46_800_000_000,
            zone: -3600,
        };
        assert_eq!(
            one_pm_plus1.time + one_pm_plus1.zone as i64 * USECS_PER_SEC,
            noon_utc.time + noon_utc.zone as i64 * USECS_PER_SEC
        );
        assert_ne!(timetz_cmp_internal(&noon_utc, &one_pm_plus1), 0);
        assert_ne!(timetz_hash(&noon_utc), timetz_hash(&one_pm_plus1));
    }

    #[test]
    fn interval_hash_uses_cmp_value_low64_then_int8() {
        let iv = Interval {
            time: 123_456_789,
            day: 5,
            month: 2,
        };
        let span = interval_cmp_value(&iv);
        let span64 = span as i64;
        assert_eq!(interval_hash(&iv), hashint8(span64));
        assert_eq!(interval_hash(&iv), hash_bytes_uint32(fold_int8(span64)));
        for seed in [0u64, 0x0123_4567_89ab_cdef] {
            assert_eq!(
                interval_hash_extended(&iv, seed),
                hashint8extended(span64, seed)
            );
        }
    }

    #[test]
    fn interval_equality_invariant() {
        use crate::interval::interval_cmp_internal;
        use types_datetime::USECS_PER_DAY;

        let a = Interval {
            time: 0,
            day: 5,
            month: 2,
        };
        let b = Interval {
            time: 0,
            day: 35,
            month: 1,
        };
        let c = Interval {
            time: 5 * USECS_PER_DAY,
            day: 60,
            month: 0,
        };

        assert_eq!(interval_cmp_internal(&a, &b), 0);
        assert_eq!(interval_cmp_internal(&a, &c), 0);
        assert_eq!(interval_hash(&a), interval_hash(&b));
        assert_eq!(interval_hash(&a), interval_hash(&c));
        let seed = 0x0123_4567_89ab_cdef;
        assert_eq!(interval_hash_extended(&a, seed), interval_hash_extended(&b, seed));
        assert_eq!(interval_hash_extended(&a, seed), interval_hash_extended(&c, seed));

        let one_month = Interval {
            time: 0,
            day: 0,
            month: 1,
        };
        let thirty_days = Interval {
            time: 0,
            day: 30,
            month: 0,
        };
        assert_eq!(interval_cmp_internal(&one_month, &thirty_days), 0);
        assert_eq!(interval_hash(&one_month), interval_hash(&thirty_days));
        let thirty_one_days = Interval {
            time: 0,
            day: 31,
            month: 0,
        };
        assert_ne!(interval_cmp_internal(&one_month, &thirty_one_days), 0);
        assert_ne!(interval_hash(&one_month), interval_hash(&thirty_one_days));
    }

    #[test]
    fn base_and_extended_are_independent_and_seed_varies() {
        let d = 7305_i32;
        let t = 43_200_000_000_i64;
        let ts = 631_152_000_000_000_i64;
        let key = TimeTzADT {
            time: t,
            zone: -3600,
        };
        let iv = Interval {
            time: 1,
            day: 2,
            month: 3,
        };

        assert_eq!(hashdateextended(d, 0) as u32, hashdate(d));
        assert_eq!(time_hash_extended(t, 0) as u32, time_hash(t));
        assert_eq!(timestamp_hash_extended(ts, 0) as u32, timestamp_hash(ts));
        assert_eq!(timestamptz_hash_extended(ts, 0) as u32, timestamptz_hash(ts));
        assert_eq!(timetz_hash_extended(&key, 0) as u32, timetz_hash(&key));
        assert_eq!(interval_hash_extended(&iv, 0) as u32, interval_hash(&iv));

        let s1 = 1_u64;
        let s2 = 0x0123_4567_89ab_cdef_u64;
        assert_ne!(hashdateextended(d, s1), hashdateextended(d, s2));
        assert_ne!(time_hash_extended(t, s1), time_hash_extended(t, s2));
        assert_ne!(timestamp_hash_extended(ts, s1), timestamp_hash_extended(ts, s2));
        assert_ne!(timetz_hash_extended(&key, s1), timetz_hash_extended(&key, s2));
        assert_ne!(interval_hash_extended(&iv, s1), interval_hash_extended(&iv, s2));
    }
}
